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

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.impl.StorageStatisticsFromIOStatistics;

/**
 * Storage statistics for S3A, dynamically generated from the IOStatistics.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AStorageStatistics
    extends StorageStatisticsFromIOStatistics {

  public static final String NAME = "S3AStorageStatistics";

  public S3AStorageStatistics(final IOStatistics ioStatistics) {
    super(NAME, "s3a", ioStatistics);
  }

  public S3AStorageStatistics() {
    super(NAME, "s3a", emptyStatistics());
  }

}
