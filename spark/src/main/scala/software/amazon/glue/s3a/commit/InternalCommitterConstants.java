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

import static software.amazon.glue.s3a.commit.CommitConstants.MAGIC;
import static software.amazon.glue.s3a.commit.CommitConstants.MAGIC_COMMITTER_ENABLED;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import software.amazon.glue.s3a.commit.magic.MagicS3GuardCommitterFactory;
import software.amazon.glue.s3a.commit.staging.DirectoryStagingCommitterFactory;
import software.amazon.glue.s3a.commit.staging.PartitionedStagingCommitterFactory;
import software.amazon.glue.s3a.commit.staging.StagingCommitterFactory;

/**
 * These are internal constants not intended for public use.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class InternalCommitterConstants {

  private InternalCommitterConstants() {
  }

  /**
   * This is the staging committer base class; only used for testing.
   */
  public static final String COMMITTER_NAME_STAGING = "staging";

  /**
   * A unique identifier to use for this work: {@value}.
   */
  public static final String FS_S3A_COMMITTER_UUID =
      "fs.s3a.committer.uuid";

  /**
   * Where did the UUID come from? {@value}.
   */
  public static final String FS_S3A_COMMITTER_UUID_SOURCE =
      "fs.s3a.committer.uuid.source";

  /**
   * Directory committer factory: {@value}.
   */
  public static final String STAGING_COMMITTER_FACTORY =
      StagingCommitterFactory.CLASSNAME;

  /**
   * Directory committer factory: {@value}.
   */
  public static final String DIRECTORY_COMMITTER_FACTORY =
      DirectoryStagingCommitterFactory.CLASSNAME;

  /**
   * Partitioned committer factory: {@value}.
   */
  public static final String PARTITION_COMMITTER_FACTORY =
      PartitionedStagingCommitterFactory.CLASSNAME;

  /**
   * Magic committer factory: {@value}.
   */
  public static final String MAGIC_COMMITTER_FACTORY =
      MagicS3GuardCommitterFactory.CLASSNAME;

  /**
   * Error text when the destination path exists and the committer
   * must abort the job/task {@value}.
   */
  public static final String E_DEST_EXISTS =
      "Destination path exists and committer conflict resolution mode is "
          + "\"fail\"";

  /** Error message for bad path: {@value}. */
  public static final String E_BAD_PATH
      = "Path does not represent a magic-commit path";

  /** Error message if filesystem isn't magic: {@value}. */
  public static final String E_NORMAL_FS
      = "Filesystem does not have support for 'magic' committer enabled"
      + " in configuration option " + MAGIC_COMMITTER_ENABLED;

  /** Error message if the dest FS isn't S3A: {@value}. */
  public static final String E_WRONG_FS
      = "Output path is not on an S3A Filesystem";

  /** Error message for a path without a magic element in the list: {@value}. */
  public static final String E_NO_MAGIC_PATH_ELEMENT
      = "No " + MAGIC + " element in path";

  /**
   * The UUID for jobs: {@value}.
   * This was historically created in Spark 1.x's SQL queries, but "went away".
   */
  public static final String SPARK_WRITE_UUID =
      "spark.sql.sources.writeJobUUID";

  /**
   * Java temp dir: {@value}.
   */
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

  /**
   * Incoming Job/task configuration didn't contain any option
   * {@link #SPARK_WRITE_UUID}.
   */
  public static final String E_NO_SPARK_UUID =
      "Job/task context does not contain a unique ID in "
          + SPARK_WRITE_UUID;

  /**
   * The MR job ID; copies from MRJobConfig so that it can be
   * referred to without needing hadoop-mapreduce on the classpath.
   */
  public static final String MR_JOB_ID = "mapreduce.job.id";

}
