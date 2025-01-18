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

import com.amazonaws.AmazonServiceException;

/**
 * Exception raised when a service was throttled.
 */
public class AWSServiceThrottledException extends AWSServiceIOException {

  /**
   * HTTP status code which signals this failure mode was triggered: {@value}.
   */
  public static final int STATUS_CODE = 503;

  /**
   * Instantiate.
   * @param operation operation which triggered this
   * @param cause the underlying cause
   */
  public AWSServiceThrottledException(String operation,
      AmazonServiceException cause) {
    super(operation, cause);
  }
}
