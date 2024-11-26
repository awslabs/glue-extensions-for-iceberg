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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.services.s3.model.CompletedPart;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.WriteOperationHelper;
import software.amazon.glue.s3a.commit.files.SinglePendingCommit;
import software.amazon.glue.s3a.statistics.PutTrackerStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.util.Preconditions;

import static software.amazon.glue.s3a.commit.magic.MagicCommitTrackerUtils.extractTaskAttemptIdFromPath;

/**
 * InMemoryMagicCommitTracker stores the commit data in memory.
 * The commit data and related data stores are flushed out from
 * the memory when the task is committed or aborted.
 */
public class InMemoryMagicCommitTracker extends MagicCommitTracker {

  /**
   * Map to store taskAttemptId, and it's corresponding list of pending commit data.
   * The entries in the Map gets removed when a task commits or aborts.
   */
  private final static Map<String, List<SinglePendingCommit>> TASK_ATTEMPT_ID_TO_MPU_METADATA = new ConcurrentHashMap<>();

  /**
   * Map to store path of the file, and it's corresponding size.
   * The entries in the Map gets removed when a task commits or aborts.
   */
  private final static Map<Path, Long> PATH_TO_BYTES_WRITTEN = new ConcurrentHashMap<>();

  /**
   * Map to store taskAttemptId, and list of paths to files written by it.
   * The entries in the Map gets removed when a task commits or aborts.
   */
  private final static Map<String, List<Path>> TASK_ATTEMPT_ID_TO_PATH = new ConcurrentHashMap<>();

  public InMemoryMagicCommitTracker(Path path,
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
        "empty/null upload ID: " + uploadId);
    Preconditions.checkArgument(parts != null, "No uploaded parts list");
    Preconditions.checkArgument(!parts.isEmpty(), "No uploaded parts to save");

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
    commitData.setIOStatistics(new IOStatisticsSnapshot(iostatistics));

    // extract the taskAttemptId from the path
    String taskAttemptId = extractTaskAttemptIdFromPath(getPath());

    // store the commit data with taskAttemptId as the key
    TASK_ATTEMPT_ID_TO_MPU_METADATA.computeIfAbsent(taskAttemptId,
        k -> Collections.synchronizedList(new ArrayList<>())).add(commitData);

    // store the byteswritten(length) for the corresponding file
    PATH_TO_BYTES_WRITTEN.put(getPath(), bytesWritten);

    // store the mapping between taskAttemptId and path
    // This information is used for removing entries from
    // the map once the taskAttempt is completed/committed.
    TASK_ATTEMPT_ID_TO_PATH.computeIfAbsent(taskAttemptId,
        k -> Collections.synchronizedList(new ArrayList<>())).add(getPath());

    LOG.info("commit metadata for {} parts in {}. size: {} byte(s) "
            + "for the taskAttemptId: {} is stored in memory",
        parts.size(), getPendingPartKey(), bytesWritten, taskAttemptId);
    LOG.debug("Closed MPU to {}, saved commit information to {}; data=:\n{}",
        getPath(), getPendingPartKey(), commitData);

    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "InMemoryMagicCommitTracker{");
    sb.append(", Number of taskAttempts=").append(TASK_ATTEMPT_ID_TO_MPU_METADATA.size());
    sb.append(", Number of files=").append(PATH_TO_BYTES_WRITTEN.size());
    sb.append('}');
    return sb.toString();
  }


  public static Map<String, List<SinglePendingCommit>> getTaskAttemptIdToMpuMetadata() {
    return TASK_ATTEMPT_ID_TO_MPU_METADATA;
  }

  public static Map<Path, Long> getPathToBytesWritten() {
    return PATH_TO_BYTES_WRITTEN;
  }

  public static Map<String, List<Path>> getTaskAttemptIdToPath() {
    return TASK_ATTEMPT_ID_TO_PATH;
  }
}
