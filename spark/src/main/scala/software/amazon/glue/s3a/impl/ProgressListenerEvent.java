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

package software.amazon.glue.s3a.impl;

/**
 * Enum for progress listener events.
 * Some are used in the {@code S3ABlockOutputStream}
 * class to manage progress; others are to assist
 * testing.
 */
public enum ProgressListenerEvent {

  /**
   * Stream has been closed.
   */
  CLOSE_EVENT,

  /** PUT operation completed successfully. */
  PUT_COMPLETED_EVENT,

  /** PUT operation was interrupted. */
  PUT_INTERRUPTED_EVENT,

  /** PUT operation was interrupted. */
  PUT_FAILED_EVENT,

  /** A PUT operation was started. */
  PUT_STARTED_EVENT,

  /** Bytes were transferred. */
  REQUEST_BYTE_TRANSFER_EVENT,

  /**
   * A multipart upload was initiated.
   */
  TRANSFER_MULTIPART_INITIATED_EVENT,

  /**
   * A multipart upload was aborted.
   */
  TRANSFER_MULTIPART_ABORTED_EVENT,

  /**
   * A multipart upload was successfully.
   */
  TRANSFER_MULTIPART_COMPLETED_EVENT,

  /**
   * An upload of a part of a multipart upload was started.
   */
  TRANSFER_PART_STARTED_EVENT,

  /**
   * An upload of a part of a multipart upload was completed.
   * This does not indicate the upload was successful.
   */
  TRANSFER_PART_COMPLETED_EVENT,

  /**
   * An upload of a part of a multipart upload was completed
   * successfully.
   */
  TRANSFER_PART_SUCCESS_EVENT,

  /**
   * An upload of a part of a multipart upload was abported.
   */
  TRANSFER_PART_ABORTED_EVENT,

  /**
   * An upload of a part of a multipart upload failed.
   */
  TRANSFER_PART_FAILED_EVENT,

}
