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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A specific exception from AWS operations.
 * The exception must always be created with an {@link AmazonServiceException}.
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
      AmazonServiceException cause) {
    super(operation, cause);
  }

  public AmazonServiceException getCause() {
    return (AmazonServiceException) super.getCause();
  }

  public String getRequestId() {
    return getCause().getRequestId();
  }

  public String getServiceName() {
    return getCause().getServiceName();
  }

  public String getErrorCode() {
    return getCause().getErrorCode();
  }

  public int getStatusCode() {
    return getCause().getStatusCode();
  }

  public String getRawResponseContent() {
    return getCause().getRawResponseContent();
  }

  public boolean isRetryable() {
    return getCause().isRetryable();
  }

}
