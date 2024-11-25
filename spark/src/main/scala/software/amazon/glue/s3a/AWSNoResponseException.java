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

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Status code 443, no response from server. This is considered idempotent.
 */
public class AWSNoResponseException extends AWSServiceIOException {
  public AWSNoResponseException(String operation,
      AwsServiceException cause) {
    super(operation, cause);
  }

  @Override
  public boolean retryable() {
    return true;
  }
}