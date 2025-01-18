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

package software.amazon.glue.s3a.commit.magic;

import static software.amazon.glue.s3a.commit.CommitConstants.X_HEADER_MAGIC_MARKER;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.WriteOperationHelper;
import software.amazon.glue.s3a.commit.PutTracker;
import software.amazon.glue.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Put tracker for Magic commits.
 * <p>Important</p>: must not directly or indirectly import a class which
 * uses any datatype in hadoop-mapreduce.
 */
@InterfaceAudience.Private
public class MagicCommitTracker extends PutTracker {
  public static final Logger LOG = LoggerFactory.getLogger(
      MagicCommitTracker.class);

  private final String originalDestKey;
  private final String pendingPartKey;
  private final Path path;
  private final WriteOperationHelper writer;
  private final String bucket;
  private static final byte[] EMPTY = new byte[0];

  /**
   * Magic commit tracker.
   * @param path path nominally being written to
   * @param bucket dest bucket
   * @param originalDestKey the original key, in the magic directory.
   * @param destKey key for the destination
   * @param pendingsetKey key of the pendingset file
   * @param writer writer instance to use for operations; includes audit span
   */
  public MagicCommitTracker(Path path,
      String bucket,
      String originalDestKey,
      String destKey,
      String pendingsetKey,
      WriteOperationHelper writer) {
    super(destKey);
    this.bucket = bucket;
    this.path = path;
    this.originalDestKey = originalDestKey;
    this.pendingPartKey = pendingsetKey;
    this.writer = writer;
    LOG.info("File {} is written as magic file to path {}",
        path, destKey);
  }

  /**
   * Initialize the tracker.
   * @return true, indicating that the multipart commit must start.
   * @throws IOException any IO problem.
   */
  @Override
  public boolean initialize() throws IOException {
    return true;
  }

  /**
   * Flag to indicate that output is not visible after the stream
   * is closed.
   * @return true
   */
  @Override
  public boolean outputImmediatelyVisible() {
    return false;
  }

  /**
   * Complete operation: generate the final commit data, put it.
   * @param uploadId Upload ID
   * @param parts list of parts
   * @param bytesWritten bytes written
   * @param iostatistics nullable IO statistics
   * @return false, indicating that the commit must fail.
   * @throws IOException any IO problem.
   * @throws IllegalArgumentException bad argument
   */
  @Override
  public boolean aboutToComplete(String uploadId,
      List<PartETag> parts,
      long bytesWritten,
      final IOStatistics iostatistics)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(uploadId),
        "empty/null upload ID: "+ uploadId);
    Preconditions.checkArgument(parts != null,
        "No uploaded parts list");
    Preconditions.checkArgument(!parts.isEmpty(),
        "No uploaded parts to save");

    // build the commit summary
    SinglePendingCommit commitData = new SinglePendingCommit();
    commitData.touch(System.currentTimeMillis());
    commitData.setDestinationKey(getDestKey());
    commitData.setBucket(bucket);
    commitData.setUri(path.toUri().toString());
    commitData.setUploadId(uploadId);
    commitData.setText("");
    commitData.setLength(bytesWritten);
    commitData.bindCommitData(parts);
    commitData.setIOStatistics(
        new IOStatisticsSnapshot(iostatistics));
    byte[] bytes = commitData.toBytes();
    LOG.info("Uncommitted data pending to file {};"
            + " commit metadata for {} parts in {}. size: {} byte(s)",
        path.toUri(), parts.size(), pendingPartKey, bytesWritten);
    LOG.debug("Closed MPU to {}, saved commit information to {}; data=:\n{}",
        path, pendingPartKey, commitData);
    PutObjectRequest put = writer.createPutObjectRequest(
        pendingPartKey,
        new ByteArrayInputStream(bytes),
        bytes.length, null);
    writer.uploadObject(put);

    // Add the final file length as a header
    Map<String, String> headers = new HashMap<>();
    headers.put(X_HEADER_MAGIC_MARKER, Long.toString(bytesWritten));
    // now put a 0-byte file with the name of the original under-magic path
    PutObjectRequest originalDestPut = writer.createPutObjectRequest(
        originalDestKey,
        new ByteArrayInputStream(EMPTY),
        0,
        headers);
    writer.uploadObject(originalDestPut);
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MagicCommitTracker{");
    sb.append(", destKey=").append(getDestKey());
    sb.append(", pendingPartKey='").append(pendingPartKey).append('\'');
    sb.append(", path=").append(path);
    sb.append(", writer=").append(writer);
    sb.append('}');
    return sb.toString();
  }
}
