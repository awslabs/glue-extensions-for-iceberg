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

package software.amazon.glue.s3a.commit;

import static software.amazon.glue.s3a.commit.CommitConstants.BASE;
import static software.amazon.glue.s3a.commit.CommitConstants.MAGIC;
import static software.amazon.glue.s3a.commit.CommitConstants.TEMP_DATA;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * These are commit utility methods which import classes from
 * hadoop-mapreduce, and so only work when that module is on the
 * classpath.
 *
 * <b>Do not use in any codepath intended to be used from the S3AFS
 * except in the committers themselves.</b>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class CommitUtilsWithMR {

  private CommitUtilsWithMR() {
  }

  /**
   * Get the location of magic job attempts.
   * @param out the base output directory.
   * @return the location of magic job attempts.
   */
  public static Path getMagicJobAttemptsPath(Path out) {
    return new Path(out, MAGIC);
  }

  /**
   * Get the Application Attempt ID for this job.
   * @param context the context to look in
   * @return the Application Attempt ID for a given job, or 0
   */
  public static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Compute the "magic" path for a job attempt.
   * @param jobUUID unique Job ID.
   * @param dest the final output directory
   * @return the path to store job attempt data.
   */
  public static Path getMagicJobAttemptPath(String jobUUID, Path dest) {
    return new Path(getMagicJobAttemptsPath(dest),
        formatAppAttemptDir(jobUUID));
  }

  /**
   * Format the application attempt directory.
   * @param jobUUID unique Job ID.
   * @return the directory name for the application attempt
   */
  public static String formatAppAttemptDir(String jobUUID) {
    return String.format("job-%s", jobUUID);
  }

  /**
   * Compute the path where the output of magic task attempts are stored.
   * @param jobUUID unique Job ID.
   * @param dest destination of work
   * @return the path where the output of magic task attempts are stored.
   */
  public static Path getMagicTaskAttemptsPath(String jobUUID, Path dest) {
    return new Path(getMagicJobAttemptPath(jobUUID, dest), "tasks");
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * This path is marked as a base path for relocations, so subdirectory
   * information is preserved.
   * @param context the context of the task attempt.
   * @param jobUUID unique Job ID.
   * @param dest The output path to commit work into
   * @return the path where a task attempt should be stored.
   */
  public static Path getMagicTaskAttemptPath(TaskAttemptContext context,
      String jobUUID,
      Path dest) {
    return new Path(getBaseMagicTaskAttemptPath(context, jobUUID, dest),
        BASE);
  }

  /**
   * Get the base Magic attempt path, without any annotations to mark relative
   * references.
   * @param context task context.
   * @param jobUUID unique Job ID.
   * @param dest The output path to commit work into
   * @return the path under which all attempts go
   */
  public static Path getBaseMagicTaskAttemptPath(TaskAttemptContext context,
      String jobUUID,
      Path dest) {
    return new Path(getMagicTaskAttemptsPath(jobUUID, dest),
          String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Compute a path for temporary data associated with a job.
   * This data is <i>not magic</i>
   * @param jobUUID unique Job ID.
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempJobAttemptPath(String jobUUID,
      Path out) {
    return new Path(new Path(out, TEMP_DATA),
        formatAppAttemptDir(jobUUID));
  }

  /**
   * Compute the path where the output of a given task attempt will be placed.
   * @param context task context
   * @param jobUUID unique Job ID.
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempTaskAttemptPath(TaskAttemptContext context,
      final String jobUUID, Path out) {
    return new Path(
        getTempJobAttemptPath(jobUUID, out),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Get a string value of a job ID; returns meaningful text if there is no ID.
   * @param context job context
   * @return a string for logs
   */
  public static String jobIdString(JobContext context) {
    JobID jobID = context.getJobID();
    return jobID != null ? jobID.toString() : "(no job ID)";
  }

  /**
   * Get a job name; returns meaningful text if there is no name.
   * @param context job context
   * @return a string for logs
   */
  public static String jobName(JobContext context) {
    String name = context.getJobName();
    return (name != null && !name.isEmpty()) ? name : "(anonymous)";
  }

  /**
   * Get a configuration option, with any value in the job configuration
   * taking priority over that in the filesystem.
   * This allows for per-job override of FS parameters.
   *
   * Order is: job context, filesystem config, default value
   *
   * @param context job/task context
   * @param fsConf filesystem configuration. Get this from the FS to guarantee
   * per-bucket parameter propagation
   * @param key key to look for
   * @param defVal default value
   * @return the configuration option.
   */
  public static String getConfigurationOption(
      JobContext context,
      Configuration fsConf,
      String key,
      String defVal) {
    return context.getConfiguration().getTrimmed(key,
        fsConf.getTrimmed(key, defVal));
  }
}
