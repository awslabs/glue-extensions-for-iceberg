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

package software.amazon.glue.s3a;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;

/**
 * The bucket or other AWS resource is unknown.
 *
 * Why not a subclass of FileNotFoundException?
 * There's too much code which caches an FNFE and infers that the file isn't
 * there - a missing bucket is far more significant and generally should
 * not be ignored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UnknownStoreException extends PathIOException {

  /**
   * Constructor.
   *
   * @param path    path trying to access.
   * @param message message.
   */
  public UnknownStoreException(final String path, final String message) {
    this(path, message, null);
  }

  /**
   * Constructor.
   *
   * @param path    path trying to access.
   * @param message message.
   * @param cause   cause (may be null).
   */
  public UnknownStoreException(String path, final String message,
      Throwable cause) {
    super(path, message);
    if (cause != null) {
      initCause(cause);
    }
  }
}
