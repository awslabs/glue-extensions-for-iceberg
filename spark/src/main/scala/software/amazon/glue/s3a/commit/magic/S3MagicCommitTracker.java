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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.Retries;
import software.amazon.glue.s3a.S3ADataBlocks;
import software.amazon.glue.s3a.WriteOperationHelper;
import software.amazon.glue.s3a.commit.files.SinglePendingCommit;
import software.amazon.glue.s3a.impl.PutObjectOptions;
import software.amazon.glue.s3a.statistics.PutTrackerStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.util.Preconditions;

import static software.amazon.glue.s3a.Statistic.COMMITTER_MAGIC_MARKER_PUT;
import static software.amazon.glue.s3a.commit.CommitConstants.X_HEADER_MAGIC_MARKER;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;

/**
 * Stores the commit data under the magic path.
 */
public class S3MagicCommitTracker extends MagicCommitTracker {

  public S3MagicCommitTracker(Path path,
      String bucket,
      String originalDestKey,
      String destKey,
      String pendingsetKey,
      WriteOperationHelper writer,
      PutTrackerStatistics trackerStatistics) {
    super(path, bucket, originalDestKey, destKey, pendingsetKey, writer, trackerStatistics);
  }

  @Override
  public boolean aboutToComplete(String uploadId,
      List<CompletedPart> parts,
      long bytesWritten,
      final IOStatistics iostatistics)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(uploadId),
        "empty/null upload ID: "+ uploadId);
    Preconditions.checkArgument(parts != null,
        "No uploaded parts list");
    Preconditions.checkArgument(!parts.isEmpty(),
        "No uploaded parts to save");

    // put a 0-byte file with the name of the original under-magic path
    // Add the final file length as a header
    // this is done before the task commit, so its duration can be
    // included in the statistics
    Map<String, String> headers = new HashMap<>();
    headers.put(X_HEADER_MAGIC_MARKER, Long.toString(bytesWritten));
    PutObjectRequest originalDestPut = getWriter().createPutObjectRequest(
        getOriginalDestKey(),
        0,
        new PutObjectOptions(true, null, headers));
    upload(originalDestPut, EMPTY);

    // build the commit summary
    SinglePendingCommit commitData = new SinglePendingCommit();
    commitData.touch(System.currentTimeMillis());
    commitData.setDestinationKey(getDestKey());
    commitData.setBucket(getBucket());
    commitData.setUri(getPath().toUri().toString());
    commitData.setUploadId(uploadId);
    commitData.setText("");
    commitData.setLength(bytesWritten);
    commitData.bindCommitData(parts);
    commitData.setIOStatistics(
        new IOStatisticsSnapshot(iostatistics));

    byte[] bytes = commitData.toBytes(SinglePendingCommit.serializer());
    LOG.info("Uncommitted data pending to file {};"
            + " commit metadata for {} parts in {}. size: {} byte(s)",
        getPath().toUri(), parts.size(), getPendingPartKey(), bytesWritten);
    LOG.debug("Closed MPU to {}, saved commit information to {}; data=:\n{}",
        getPath(), getPendingPartKey(), commitData);
    PutObjectRequest put = getWriter().createPutObjectRequest(
        getPendingPartKey(),
        bytes.length, null);
    upload(put, bytes);
    return false;
  }

  /**
   * PUT an object.
   * @param request the request
   * @param inputStream input stream of data to be uploaded
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  private void upload(PutObjectRequest request, byte[] bytes) throws IOException {
    trackDurationOfInvocation(getTrackerStatistics(), COMMITTER_MAGIC_MARKER_PUT.getSymbol(),
        () -> getWriter().putObject(request, PutObjectOptions.keepingDirs(),
            new S3ADataBlocks.BlockUploadData(bytes, null), null));
  }
}
