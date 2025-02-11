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

import static software.amazon.glue.s3a.commit.InternalCommitterConstants.*;
import static software.amazon.glue.s3a.commit.ValidationFailure.verify;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static utility methods related to S3A commitment processing, both
 * staging and magic.
 *
 * <b>Do not use in any codepath intended to be used from the S3AFS
 * except in the committers themselves.</b>
 */
public final class CommitUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(CommitUtils.class);

  private CommitUtils() {
  }

  /**
   * Verify that the path is a magic one.
   * @param fs filesystem
   * @param path path
   * @throws PathCommitException if the path isn't a magic commit path
   */
  public static void verifyIsMagicCommitPath(S3AFileSystem fs,
      Path path) throws PathCommitException {
    verifyIsMagicCommitFS(fs);
    if (!fs.isMagicCommitPath(path)) {
      throw new PathCommitException(path, E_BAD_PATH);
    }
  }

  /**
   * Verify that an S3A FS instance is a magic commit FS.
   * @param fs filesystem
   * @throws PathCommitException if the FS isn't a magic commit FS.
   */
  public static void verifyIsMagicCommitFS(S3AFileSystem fs)
      throws PathCommitException {
    if (!fs.isMagicCommitEnabled()) {
      // dump out details to console for support diagnostics
      String fsUri = fs.getUri().toString();
      LOG.error("{}: {}:\n{}", E_NORMAL_FS, fsUri, fs);
      // then fail
      throw new PathCommitException(fsUri, E_NORMAL_FS);
    }
  }

  /**
   * Verify that an FS is an S3A FS.
   * @param fs filesystem
   * @param path path to to use in exception
   * @return the typecast FS.
   * @throws PathCommitException if the FS is not an S3A FS.
   */
  public static S3AFileSystem verifyIsS3AFS(FileSystem fs, Path path)
      throws PathCommitException {
    if (!(fs instanceof S3AFileSystem)) {
      throw new PathCommitException(path, E_WRONG_FS);
    }
    return (S3AFileSystem) fs;
  }

  /**
   * Get the S3A FS of a path.
   * @param path path to examine
   * @param conf config
   * @param magicCommitRequired is magic complete required in the FS?
   * @return the filesystem
   * @throws PathCommitException output path isn't to an S3A FS instance, or
   * if {@code magicCommitRequired} is set, if doesn't support these commits.
   * @throws IOException failure to instantiate the FS
   */
  public static S3AFileSystem getS3AFileSystem(Path path,
      Configuration conf,
      boolean magicCommitRequired)
      throws PathCommitException, IOException {
    S3AFileSystem s3AFS = verifyIsS3AFS(path.getFileSystem(conf), path);
    if (magicCommitRequired) {
      verifyIsMagicCommitFS(s3AFS);
    }
    return s3AFS;
  }

  /**
   * Verify that all instances in a collection are of the given class.
   * @param it iterator
   * @param classname classname to require
   * @throws ValidationFailure on a failure
   */
  public static void validateCollectionClass(Iterable it, Class classname)
      throws ValidationFailure {
    for (Object o : it) {
      verify(o.getClass().equals(classname),
          "Collection element is not a %s: %s", classname, o.getClass());
    }
  }

  /**
   * Extract the job ID from a configuration.
   * @param conf configuration
   * @return a job ID or null.
   */
  public static String extractJobID(Configuration conf) {

    String jobUUID = conf.getTrimmed(FS_S3A_COMMITTER_UUID, "");

    if (!jobUUID.isEmpty()) {
      return jobUUID;
    }
    // there is no job UUID.
    // look for one from spark
    jobUUID = conf.getTrimmed(SPARK_WRITE_UUID, "");
    if (!jobUUID.isEmpty()) {
      return jobUUID;
    }
    jobUUID = conf.getTrimmed(MR_JOB_ID, "");
    if (!jobUUID.isEmpty()) {
      return jobUUID;
    }
    return null;
  }

}
