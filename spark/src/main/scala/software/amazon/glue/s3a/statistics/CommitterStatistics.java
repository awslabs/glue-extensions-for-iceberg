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

package software.amazon.glue.s3a.statistics;

/**
 * Statistics for S3A committers.
 */
public interface CommitterStatistics
    extends S3AStatisticInterface {

  /** A commit has been created. */
  void commitCreated();

  /**
   * Data has been uploaded to be committed in a subsequent operation.
   * @param size size in bytes
   */
  void commitUploaded(long size);

  /**
   * A commit has been completed.
   * @param size size in bytes
   */
  void commitCompleted(long size);

  /** A commit has been aborted. */
  void commitAborted();

  /**
   * A commit was reverted.
   */
  void commitReverted();

  /**
   * A commit failed.
   */
  void commitFailed();

  /**
   * Note that a task has completed.
   * @param success success flag
   */
  void taskCompleted(boolean success);

  /**
   * Note that a job has completed.
   * @param success success flag
   */
  void jobCompleted(boolean success);
}
