/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.glue.s3a.prefetch;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.impl.prefetch.BlockManager;
import org.apache.hadoop.fs.impl.prefetch.BlockManagerParameters;
import org.apache.hadoop.fs.impl.prefetch.BufferData;
import org.apache.hadoop.fs.impl.prefetch.FilePosition;
import software.amazon.glue.s3a.S3AInputStream;
import software.amazon.glue.s3a.S3AReadOpContext;
import software.amazon.glue.s3a.S3ObjectAttributes;
import software.amazon.glue.s3a.statistics.S3AInputStreamStatistics;

import static software.amazon.glue.s3a.Constants.DEFAULT_PREFETCH_MAX_BLOCKS_COUNT;
import static software.amazon.glue.s3a.Constants.PREFETCH_MAX_BLOCKS_COUNT;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_ACQUIRE_AND_READ;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration;

/**
 * Provides an {@code InputStream} that allows reading from an S3 file.
 * Prefetched blocks are cached to local disk if a seek away from the
 * current block is issued.
 */
public class S3ACachingInputStream extends S3ARemoteInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ACachingInputStream.class);

  /**
   * Number of blocks queued for prefching.
   */
  private final int numBlocksToPrefetch;

  private final BlockManager blockManager;

  /**
   * Initializes a new instance of the {@code S3ACachingInputStream} class.
   *
   * @param context read-specific operation context.
   * @param s3Attributes attributes of the S3 object being read.
   * @param client callbacks used for interacting with the underlying S3 client.
   * @param streamStatistics statistics for this stream.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance.
   * @throws IllegalArgumentException if context is null.
   * @throws IllegalArgumentException if s3Attributes is null.
   * @throws IllegalArgumentException if client is null.
   */
  public S3ACachingInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client,
      S3AInputStreamStatistics streamStatistics,
      Configuration conf,
      LocalDirAllocator localDirAllocator) {
    super(context, s3Attributes, client, streamStatistics);

    this.numBlocksToPrefetch = this.getContext().getPrefetchBlockCount();
    int bufferPoolSize = this.numBlocksToPrefetch + 1;
    BlockManagerParameters blockManagerParamsBuilder =
        new BlockManagerParameters()
            .withFuturePool(this.getContext().getFuturePool())
            .withBlockData(this.getBlockData())
            .withBufferPoolSize(bufferPoolSize)
            .withConf(conf)
            .withLocalDirAllocator(localDirAllocator)
            .withMaxBlocksCount(
                conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
            .withPrefetchingStatistics(getS3AStreamStatistics())
            .withTrackerFactory(getS3AStreamStatistics());
    this.blockManager = this.createBlockManager(blockManagerParamsBuilder,
        this.getReader());
    int fileSize = (int) s3Attributes.getLen();
    LOG.debug("Created caching input stream for {} (size = {})", this.getName(),
        fileSize);
  }

  @Override
  public void close() throws IOException {
    // Close the BlockManager first, cancelling active prefetches,
    // deleting cached files and freeing memory used by buffer pool.
    blockManager.close();
    super.close();
    LOG.info("closed: {}", getName());
  }

  @Override
  protected boolean ensureCurrentBuffer() throws IOException {
    if (isClosed()) {
      return false;
    }

    long readPos = getNextReadPos();
    if (!getBlockData().isValidOffset(readPos)) {
      return false;
    }

    // Determine whether this is an out of order read.
    FilePosition filePosition = getFilePosition();
    boolean outOfOrderRead = !filePosition.setAbsolute(readPos);

    if (!outOfOrderRead && filePosition.buffer().hasRemaining()) {
      // Use the current buffer.
      return true;
    }

    if (filePosition.isValid()) {
      // We are jumping out of the current buffer. There are two cases to consider:
      if (filePosition.bufferFullyRead()) {
        // This buffer was fully read:
        // it is very unlikely that this buffer will be needed again;
        // therefore we release the buffer without caching.
        blockManager.release(filePosition.data());
      } else {
        // We will likely need this buffer again (as observed empirically for Parquet)
        // therefore we issue an async request to cache this buffer.
        blockManager.requestCaching(filePosition.data());
      }
      filePosition.invalidate();
    }

    int prefetchCount;
    if (outOfOrderRead) {
      LOG.debug("lazy-seek({})", getOffsetStr(readPos));
      blockManager.cancelPrefetches();

      // We prefetch only 1 block immediately after a seek operation.
      prefetchCount = 1;
    } else {
      // A sequential read results in a prefetch.
      prefetchCount = numBlocksToPrefetch;
    }

    int toBlockNumber = getBlockData().getBlockNumber(readPos);
    long startOffset = getBlockData().getStartOffset(toBlockNumber);

    for (int i = 1; i <= prefetchCount; i++) {
      int b = toBlockNumber + i;
      if (b < getBlockData().getNumBlocks()) {
        blockManager.requestPrefetch(b);
      }
    }

    BufferData data = invokeTrackingDuration(
        getS3AStreamStatistics()
            .trackDuration(STREAM_READ_BLOCK_ACQUIRE_AND_READ),
        () -> blockManager.get(toBlockNumber));

    filePosition.setData(data, startOffset, readPos);
    return true;
  }

  @Override
  public String toString() {
    if (isClosed()) {
      return "closed";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%s%n", super.toString()));
    sb.append(blockManager.toString());
    return sb.toString();
  }

  protected BlockManager createBlockManager(
      @Nonnull final BlockManagerParameters blockManagerParameters,
      final S3ARemoteObjectReader reader) {
    return new S3ACachingBlockManager(blockManagerParameters, reader);
  }
}
