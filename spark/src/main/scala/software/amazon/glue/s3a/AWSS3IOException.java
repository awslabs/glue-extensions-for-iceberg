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

import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Wrap a {@link S3Exception} as an IOE, relaying all
 * getters.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AWSS3IOException extends AWSServiceIOException {

  /**
   * Instantiate.
   * @param operation operation which triggered this
   * @param cause the underlying cause
   */
  public AWSS3IOException(String operation,
      S3Exception cause) {
    super(operation, cause);
  }

  public S3Exception getCause() {
    return (S3Exception) super.getCause();
  }

}
