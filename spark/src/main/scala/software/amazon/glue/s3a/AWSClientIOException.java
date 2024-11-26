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

import software.amazon.awssdk.core.exception.SdkException;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;

/**
 * IOException equivalent of an {@link SdkException}.
 */
public class AWSClientIOException extends IOException {

  private final String operation;

  public AWSClientIOException(String operation,
      SdkException cause) {
    super(cause);
    Preconditions.checkArgument(operation != null, "Null 'operation' argument");
    Preconditions.checkArgument(cause != null, "Null 'cause' argument");
    this.operation = operation;
  }

  public SdkException getCause() {
    return (SdkException) super.getCause();
  }

  @Override
  public String getMessage() {
    return operation + ": " + getCause().getMessage();
  }

  /**
   * Query inner cause for retryability.
   * @return what the cause says.
   */
  public boolean retryable() {
    return getCause().retryable();
  }
}
