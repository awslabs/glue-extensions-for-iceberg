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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Enum of performance flags.
 * <p>
 * When adding new flags, please keep in alphabetical order.
 */
@InterfaceAudience.LimitedPrivate("S3A Filesystem and extensions")
@InterfaceStability.Unstable
public enum PerformanceFlagEnum {
  /**
   * Create performance.
   */
  Create,

  /**
   * Delete performance.
   */
  Delete,

  /**
   * Mkdir performance.
   */
  Mkdir,

  /**
   * Open performance.
   */
  Open
}
