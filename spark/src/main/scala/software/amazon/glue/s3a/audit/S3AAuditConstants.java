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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.LimitedPrivate("S3A auditing extensions")
@InterfaceStability.Unstable
public final class S3AAuditConstants {

  private S3AAuditConstants() {
  }

  /**
   * What to look for in logs for ops outside any audit.
   * {@value}.
   */
  public static final String UNAUDITED_OPERATION = "unaudited operation";

  /**
   * Is auditing enabled?
   * Value: {@value}.
   */
  public static final String AUDIT_ENABLED = "fs.s3a.audit.enabled";

  /**
   * Default auditing flag.
   * Value: {@value}.
   */
  public static final boolean AUDIT_ENABLED_DEFAULT = false;


  /**
   * Name of class used for audit logs: {@value}.
   */
  public static final String AUDIT_SERVICE_CLASSNAME =
      "fs.s3a.audit.service.classname";

  /**
   * Classname of the logging auditor: {@value}.
   */
  public static final String LOGGING_AUDIT_SERVICE =
      "software.amazon.glue.s3a.audit.impl.LoggingAuditor";

  /**
   * Classname of the No-op auditor: {@value}.
   */
  public static final String NOOP_AUDIT_SERVICE =
      "software.amazon.glue.s3a.audit.impl.NoopAuditor";

  /**
   * List of extra AWS SDK request handlers: {@value}.
   * These are added to the SDK request chain <i>after</i>
   * any audit service.
   */
  public static final String AUDIT_REQUEST_HANDLERS =
      "fs.s3a.audit.request.handlers";

  /**
   * Should operations outside spans be rejected?
   * This is for testing coverage of the span code; if used
   * in production there's a risk of unexpected failures.
   * {@value}.
   */
  public static final String REJECT_OUT_OF_SPAN_OPERATIONS
      = "fs.s3a.audit.reject.out.of.span.operations";

  /**
   * Should the logging auditor add the HTTP Referrer header?
   * {@value}.
   */
  public static final String REFERRER_HEADER_ENABLED
      = "fs.s3a.audit.referrer.enabled";

  /**
   * Should the logging auditor add the HTTP Referrer header?
   * Default value: {@value}.
   */
  public static final boolean REFERRER_HEADER_ENABLED_DEFAULT
      = true;

  /**
   * List of audit fields to strip from referrer headers.
   * {@value}.
   */
  public static final String REFERRER_HEADER_FILTER
      = "fs.s3a.audit.referrer.filter";

  /**
   * Span name used during initialization.
   */
  public static final String INITIALIZE_SPAN = "initialize";

  /**
   * Operation name for any operation outside of an explicit
   * span.
   */
  public static final String OUTSIDE_SPAN =
      "outside-span";
}
