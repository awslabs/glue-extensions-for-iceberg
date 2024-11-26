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

import java.io.EOFException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Http channel exception; subclass of EOFException.
 * In particular:
 * - NoHttpResponseException
 * - OpenSSL errors
 * The http client library exceptions may be shaded/unshaded; this is the
 * exception used in retry policies.
 */
@InterfaceAudience.Private
public class HttpChannelEOFException extends EOFException {

  public HttpChannelEOFException(final String path,
      final String error,
      final Throwable cause) {
    super(error);
    initCause(cause);
  }
}
