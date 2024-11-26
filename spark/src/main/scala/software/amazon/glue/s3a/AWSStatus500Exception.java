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
 * A 5xx response came back from a service.
 * <p>
 * The 500 error is considered retryable by the AWS SDK, which will have already
 * retried it {@code fs.s3a.attempts.maximum} times before reaching s3a
 * code.
 * <p>
 * These are rare, but can occur; they are considered retryable.
 * Note that HADOOP-19221 shows a failure condition where the
 * SDK itself did not recover on retry from the error.
 * In S3A code, retries happen if the retry policy configuration
 * {@code fs.s3a.retry.http.5xx.errors} is {@code true}.
 * <p>
 * In third party stores it may have a similar meaning -though it
 * can often just mean "misconfigured server".
 */
public class AWSStatus500Exception extends AWSServiceIOException {
  public AWSStatus500Exception(String operation,
      AwsServiceException cause) {
    super(operation, cause);
  }

}
