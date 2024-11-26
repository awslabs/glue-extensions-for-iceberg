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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.commit.CommitConstants;
import software.amazon.glue.s3a.commit.MagicCommitPaths;

import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Utility class for the class {@link MagicCommitTracker} and its subclasses.
 */
public final class MagicCommitTrackerUtils {

  private MagicCommitTrackerUtils() {
  }

  /**
   * The magic path is of the following format.
   * "s3://bucket-name/table-path/__magic_jobId/job-id/taskAttempt/id/tasks/taskAttemptId"
   * So the third child from the "__magic" path will give the task attempt id.
   * @param path Path
   * @return taskAttemptId
   */
  public static String extractTaskAttemptIdFromPath(Path path) {
    List<String> elementsInPath = MagicCommitPaths.splitPathToElements(path);
    List<String> childrenOfMagicPath = MagicCommitPaths.magicPathChildren(elementsInPath);

    checkArgument(childrenOfMagicPath.size() >= 3, "Magic Path is invalid");
    // 3rd child of the magic path is the taskAttemptId
    return childrenOfMagicPath.get(3);
  }

  /**
   * Is tracking of magic commit data in-memory enabled.
   * @param conf Configuration
   * @return true if in memory tracking of commit data is enabled.
   */
  public static boolean isTrackMagicCommitsInMemoryEnabled(Configuration conf) {
    return conf.getBoolean(
        CommitConstants.FS_S3A_COMMITTER_MAGIC_TRACK_COMMITS_IN_MEMORY_ENABLED,
        CommitConstants.FS_S3A_COMMITTER_MAGIC_TRACK_COMMITS_IN_MEMORY_ENABLED_DEFAULT);
  }
}
