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

package software.amazon.glue.s3a.api;

import org.apache.hadoop.fs.PathIOException;

/**
 * An operation is unsupported.
 */
public class UnsupportedRequestException extends PathIOException {

  public UnsupportedRequestException(final String path, final Throwable cause) {
    super(path, cause);
  }

  public UnsupportedRequestException(final String path, final String error) {
    super(path, error);
  }

  public UnsupportedRequestException(final String path,
      final String error,
      final Throwable cause) {
    super(path, error, cause);
  }
}
