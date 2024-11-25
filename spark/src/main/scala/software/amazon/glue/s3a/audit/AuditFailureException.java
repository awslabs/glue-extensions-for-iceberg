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

package software.amazon.glue.s3a.audit;

import software.amazon.glue.s3a.CredentialInitializationException;

/**
 * This is in the AWS exception tree so that exceptions raised in the
 * AWS SDK are correctly reported up.
 * It is a subclass of {@link CredentialInitializationException}
 * so that
 * {@code S3AUtils.translateException()} recognizes these exceptions
 * and converts them to AccessDeniedException.
 */
public class AuditFailureException extends CredentialInitializationException {

  public AuditFailureException(final String message, final Throwable t) {
    super(message, t);
  }

  public AuditFailureException(final String message) {
    super(message);
  }

}
