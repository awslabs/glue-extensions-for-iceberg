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

import software.amazon.glue.s3a.s3guard.MetastoreInstrumentation;

/**
 * This is the statistics context for ongoing operations in S3A.
 */
public interface S3AStatisticsContext extends CountersAndGauges {

  /**
   * Get the metastore instrumentation.
   * @return an instance of the metastore statistics tracking.
   */
  MetastoreInstrumentation getS3GuardInstrumentation();

  /**
   * Create a stream input statistics instance.
   * @return the new instance
   */
  S3AInputStreamStatistics newInputStreamStatistics();

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  CommitterStatistics newCommitterStatistics();

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  BlockOutputStreamStatistics newOutputStreamStatistics();

  /**
   * Create a delegation token statistics instance.
   * @return an instance of delegation token statistics
   */
  DelegationTokenStatistics newDelegationTokenStatistics();

  /**
   * Create a StatisticsFromAwsSdk instance.
   * @return an instance of StatisticsFromAwsSdk
   */
  StatisticsFromAwsSdk newStatisticsFromAwsSdk();

  /**
   * Creaet a multipart statistics collector.
   * @return an instance
   */
  S3AMultipartUploaderStatistics createMultipartUploaderStatistics();
}
