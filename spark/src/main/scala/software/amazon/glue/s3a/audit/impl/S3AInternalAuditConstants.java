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

package software.amazon.glue.s3a.audit.impl;

import com.amazonaws.handlers.HandlerContextKey;
import org.apache.hadoop.classification.InterfaceAudience;
import software.amazon.glue.s3a.audit.AWSAuditEventCallbacks;

/**
 * Internal constants; not intended for public use, or
 * for use by any external implementations.
 */
@InterfaceAudience.Private
public final class S3AInternalAuditConstants {

  private S3AInternalAuditConstants() {
  }

  /**
   * Handler key for audit span callbacks.
   * This is used to bind the handler in the AWS code.
   */
  public static final HandlerContextKey<AWSAuditEventCallbacks>
      AUDIT_SPAN_HANDLER_CONTEXT =
      new HandlerContextKey<>(
          "software.amazon.glue.s3a.audit.AWSAuditEventCallbacks");
}
