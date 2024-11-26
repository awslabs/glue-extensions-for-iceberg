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

import software.amazon.glue.s3a.AWSClientIOException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * A specific exception from AWS operations.
 * The exception must always be created with an {@link AwsServiceException}.
 * The attributes of this exception can all be directly accessed.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AWSServiceIOException extends AWSClientIOException {

  /**
   * Instantiate.
   * @param operation operation which triggered this
   * @param cause the underlying cause
   */
  public AWSServiceIOException(String operation,
      AwsServiceException cause) {
    super(operation, cause);
  }

  public AwsServiceException getCause() {
    return (AwsServiceException) super.getCause();
  }

  public String requestId() {
    return getCause().requestId();
  }

  public AwsErrorDetails awsErrorDetails() {
    return getCause().awsErrorDetails();
  }

  public int statusCode() {
    return getCause().statusCode();
  }

  public String extendedRequestId() {
    return getCause().extendedRequestId();
  }
}
