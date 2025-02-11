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

package software.amazon.glue.s3a.statistics.impl;

import static software.amazon.glue.s3a.Statistic.STORE_IO_REQUEST;
import static software.amazon.glue.s3a.Statistic.STORE_IO_RETRY;
import static software.amazon.glue.s3a.Statistic.STORE_IO_THROTTLED;
import static software.amazon.glue.s3a.Statistic.STORE_IO_THROTTLE_RATE;

import java.time.Duration;
import software.amazon.glue.s3a.statistics.CountersAndGauges;
import software.amazon.glue.s3a.statistics.StatisticsFromAwsSdk;

/**
 * Hook up AWS SDK Statistics to the S3 counters.
 * <p>
 * Durations are not currently being used; that could be
 * changed in future once an effective strategy for reporting
 * them is determined.
 */
public final class StatisticsFromAwsSdkImpl implements
    StatisticsFromAwsSdk {

  private final CountersAndGauges countersAndGauges;

  public StatisticsFromAwsSdkImpl(
      final CountersAndGauges countersAndGauges) {
    this.countersAndGauges = countersAndGauges;
  }

  @Override
  public void updateAwsRequestCount(final long count) {
    countersAndGauges.incrementCounter(STORE_IO_REQUEST, count);
  }

  @Override
  public void updateAwsRetryCount(final long count) {
    countersAndGauges.incrementCounter(STORE_IO_RETRY, count);
  }

  @Override
  public void updateAwsThrottleExceptionsCount(final long count) {
    countersAndGauges.incrementCounter(STORE_IO_THROTTLED, count);
    countersAndGauges.addValueToQuantiles(STORE_IO_THROTTLE_RATE, count);
  }

  @Override
  public void noteAwsRequestTime(final Duration duration) {

  }

  @Override
  public void noteAwsClientExecuteTime(final Duration duration) {

  }

  @Override
  public void noteRequestMarshallTime(final Duration duration) {

  }

  @Override
  public void noteRequestSigningTime(final Duration duration) {

  }

  @Override
  public void noteResponseProcessingTime(final Duration duration) {

  }
}
