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

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 *   Annotations to inform the caller of an annotated method whether
 *   the method performs retries and/or exception translation internally.
 *   Callers should use this information to inform their own decisions about
 *   performing retries or exception translation when calling the method. For
 *   example, if a method is annotated {@code RetryTranslated}, the caller
 *   MUST NOT perform another layer of retries.  Similarly, the caller shouldn't
 *   perform another layer of exception translation.
 * </p>
 * <p>
 *   Declaration for documentation only.
 *   This is purely for visibility in source and is currently package-scoped.
 *   Compare with {@link org.apache.hadoop.io.retry.AtMostOnce}
 *   and {@link org.apache.hadoop.io.retry.Idempotent}; these are real
 *   markers used by Hadoop RPC.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Retries {
  /**
   * No retry, exceptions are translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceTranslated {
    String value() default "";
  }

  /**
   * No retry, exceptions are not translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceRaw {
    String value() default "";
  }

  /**
   * No retry, expect a bit of both.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceMixed {
    String value() default "";
  }

  /**
   * Retried, exceptions are translated.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryTranslated {
    String value() default "";
  }

  /**
   * Retried, no translation.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryRaw {
    String value() default "";
  }

  /**
   * Retried, mixed translation.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryMixed {
    String value() default "";
  }


  /**
   * Retried, Exceptions are swallowed.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface RetryExceptionsSwallowed {

    String value() default "";
  }

  /**
   * One attempt, Exceptions are swallowed.
   */
  @Documented
  @Retention(RetentionPolicy.SOURCE)
  public @interface OnceExceptionsSwallowed {

    String value() default "";
  }

}
