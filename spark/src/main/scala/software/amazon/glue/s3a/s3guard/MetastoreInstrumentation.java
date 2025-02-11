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
 * Instrumentation exported to S3Guard.
 */
public interface MetastoreInstrumentation {

  /** Initialized event. */
  void initialized();

  /** Store has been closed. */
  void storeClosed();

  /**
   * Throttled request.
   */
  void throttled();

  /**
   * S3Guard is retrying after a (retryable) failure.
   */
  void retrying();

  /**
   * Records have been deleted.
   * @param count the number of records deleted.
   */
  void recordsDeleted(int count);

  /**
   * Records have been read.
   * @param count the number of records read
   */
  void recordsRead(int count);

  /**
   * records have been written (including tombstones).
   * @param count number of records written.
   */
  void recordsWritten(int count);

  /**
   * A directory has been tagged as authoritative.
   */
  void directoryMarkedAuthoritative();

  /**
   * An entry was added.
   * @param durationNanos time to add
   */
  void entryAdded(long durationNanos);
}
