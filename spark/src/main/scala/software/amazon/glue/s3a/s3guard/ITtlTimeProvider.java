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

package software.amazon.glue.s3a.s3guard;

/**
 * This interface is defined for handling TTL expiry of metadata in S3Guard.
 *
 * TTL can be tested by implementing this interface and setting is as
 * {@code S3Guard.ttlTimeProvider}. By doing this, getNow() can return any
 * value preferred and flaky tests could be avoided. By default getNow()
 * returns the EPOCH in runtime.
 *
 * Time is measured in milliseconds,
 */
public interface ITtlTimeProvider {

  /**
   * The current time in milliseconds.
   * Assuming this calls System.currentTimeMillis(), this is a native iO call
   * and so should be invoked sparingly (i.e. evaluate before any loop, rather
   * than inside).
   * @return the current time.
   */
  long getNow();

  /**
   * The TTL of the metadata.
   * @return time in millis after which metadata is considered out of date.
   */
  long getMetadataTtl();
}
