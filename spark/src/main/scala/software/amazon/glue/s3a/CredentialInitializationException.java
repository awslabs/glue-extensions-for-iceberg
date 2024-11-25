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

import software.amazon.awssdk.core.exception.SdkClientException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Exception which Hadoop's AWSCredentialsProvider implementations should
 * throw when there is a problem with the credential setup. This
 * is a subclass of {@link SdkClientException} which sets
 * {@link #retryable()} to false, so as to fail fast.
 * This is used in credential providers and elsewhere.
 * When passed through {@code S3AUtils.translateException()} it
 * is mapped to an AccessDeniedException.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CredentialInitializationException extends SdkClientException {

  public CredentialInitializationException(String message, Throwable t) {
    super(builder().message(message).cause(t));
  }

  public CredentialInitializationException(String message) {
    super(builder().message(message));
  }

  /**
   * This exception is not going to go away if you try calling it again.
   * @return false, always.
   */
  @Override
  public boolean retryable() {
    return false;
  }
}
