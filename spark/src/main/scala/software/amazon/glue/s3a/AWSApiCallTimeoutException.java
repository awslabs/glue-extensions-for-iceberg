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

import org.apache.hadoop.net.ConnectTimeoutException;

/**
 * IOException equivalent of an {@code ApiCallTimeoutException}.
 * Declared as a subclass of {@link ConnectTimeoutException} to allow
 * for existing code to catch it.
 */
public class AWSApiCallTimeoutException extends ConnectTimeoutException {

  /**
   * Constructor.
   * @param operation operation in progress
   * @param cause cause.
   */
  public AWSApiCallTimeoutException(
      final String operation,
      final Exception cause) {
    super(operation);
    initCause(cause);
  }
}
