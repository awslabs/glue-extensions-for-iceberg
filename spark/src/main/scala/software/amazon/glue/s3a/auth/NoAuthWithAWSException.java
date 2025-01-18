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

package software.amazon.glue.s3a.auth;

import software.amazon.glue.s3a.CredentialInitializationException;

/**
 * A specific subclass of {@code AmazonClientException} which is
 * used in the S3A retry policy to fail fast when there is any
 * authentication problem.
 */
public class NoAuthWithAWSException extends CredentialInitializationException {

  public NoAuthWithAWSException(final String message, final Throwable t) {
    super(message, t);
  }

  public NoAuthWithAWSException(final String message) {
    super(message);
  }
}
