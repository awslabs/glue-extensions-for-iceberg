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

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;


/**
 * Callbacks for audit spans. This is implemented
 * in the span manager as well as individual audit spans.
 * If any of the code in a callback raises an InterruptedException,
 * it must be caught and {@code Thread.interrupt()} called to
 * redeclare the thread as interrupted. The AWS SDK will
 * detect this and raise an exception.
 *
 * Look at the documentation for
 * {@code ExecutionInterceptor} for details
 * on the callbacks.
 */
public interface AWSAuditEventCallbacks extends ExecutionInterceptor {

  /**
   * Return a span ID which must be unique for all spans within
   * everywhere. That effectively means part of the
   * span SHOULD be derived from a UUID.
   * Callers MUST NOT make any assumptions about the actual
   * contents or structure of this string other than the
   * uniqueness.
   * @return a non-empty string
   */
  String getSpanId();

  /**
   * Get the name of the operation.
   * @return the operation name.
   */
  String getOperationName();

  /**
   * Callback when a request is created in the S3A code.
   * This is called in {@code RequestFactoryImpl} after
   * each request is created.
   * It is not invoked on any AWS requests created in the SDK.
   * Avoid raising exceptions or talking to any remote service;
   * this callback is for annotation rather than validation.
   * @param builder the request builder.
   */
  default void requestCreated(SdkRequest.Builder builder) {}

}
