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

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkBaseException;
import java.io.IOException;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * IOException equivalent of an {@link AmazonClientException}.
 */
public class AWSClientIOException extends IOException {

  private final String operation;

  public AWSClientIOException(String operation,
      SdkBaseException cause) {
    super(cause);
    Preconditions.checkArgument(operation != null, "Null 'operation' argument");
    Preconditions.checkArgument(cause != null, "Null 'cause' argument");
    this.operation = operation;
  }

  public AmazonClientException getCause() {
    return (AmazonClientException) super.getCause();
  }

  @Override
  public String getMessage() {
    return operation + ": " + getCause().getMessage();
  }

}
