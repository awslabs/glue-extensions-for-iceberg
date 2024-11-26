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
 * Indicates the S3 object does not provide the versioning attribute required
 * by the configured change detection policy.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class NoVersionAttributeException extends PathIOException {

  /**
   * Constructs a NoVersionAttributeException.
   *
   * @param path the path accessed when the condition was detected
   * @param message a message providing more details about the condition
   */
  public NoVersionAttributeException(String path,
      String message) {
    super(path, message);
  }
}
