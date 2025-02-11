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

import static software.amazon.glue.s3a.Constants.BUFFER_DIR;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.Constants;

/**
 * A class which manages access to a temporary directory store, uses the
 * directories listed in {@link Constants#BUFFER_DIR} for this.
 */
final class LocalTempDir {

  private LocalTempDir() {
  }

  private static LocalDirAllocator directoryAllocator;

  private static synchronized LocalDirAllocator getAllocator(
      Configuration conf, String key) {
    if (directoryAllocator != null) {
      String bufferDir = conf.get(key) != null
          ? key : Constants.HADOOP_TMP_DIR;
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }
    return directoryAllocator;
  }

  /**
   * Create a temp file.
   * @param conf configuration to use when creating the allocator
   * @param prefix filename prefix
   * @param size file size, or -1 if not known
   * @return the temp file. The file has been created.
   * @throws IOException IO failure
   */
  public static File tempFile(Configuration conf, String prefix, long size)
      throws IOException {
    return getAllocator(conf, BUFFER_DIR).createTmpFileForWrite(
        prefix, size, conf);
  }

  /**
   * Get a temporary path.
   * @param conf configuration to use when creating the allocator
   * @param prefix filename prefix
   * @param size file size, or -1 if not known
   * @return the temp path.
   * @throws IOException IO failure
   */
  public static Path tempPath(Configuration conf, String prefix, long size)
      throws IOException {
    return getAllocator(conf, BUFFER_DIR)
        .getLocalPathForWrite(prefix, size, conf);
  }

}
