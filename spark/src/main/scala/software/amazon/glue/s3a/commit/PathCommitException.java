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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

/**
 * Path exception to use for various commit issues.
 */
public class PathCommitException extends PathIOException {
  public PathCommitException(String path, Throwable cause) {
    super(path, cause);
  }

  public PathCommitException(String path, String error) {
    super(path, error);
  }

  public PathCommitException(Path path, String error) {
    super(path != null ? path.toString() : "", error);
  }

  public PathCommitException(String path, String error, Throwable cause) {
    super(path, error, cause);
  }
}
