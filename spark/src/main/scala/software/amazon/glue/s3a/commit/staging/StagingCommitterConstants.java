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

package software.amazon.glue.s3a.commit.staging;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Internal staging committer constants.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class StagingCommitterConstants {

  private StagingCommitterConstants() {
  }

  /**
   * The temporary path for staging data, if not explicitly set.
   * By using an unqualified path, this will be qualified to be relative
   * to the users' home directory, so protected from access for others.
   */
  public static final String FILESYSTEM_TEMP_PATH = "tmp/staging";

  /** Name of the root partition :{@value}. */
  public static final String TABLE_ROOT = "table_root";

  /**
   * Filename used under {@code ~/${UUID}} for the staging files.
   */
  public static final String STAGING_UPLOADS = "staging-uploads";

}
