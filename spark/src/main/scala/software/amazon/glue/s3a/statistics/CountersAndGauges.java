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

import java.time.Duration;
import software.amazon.glue.s3a.Statistic;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * This is the foundational API for collecting S3A statistics.
 */
public interface CountersAndGauges extends DurationTrackerFactory {

  /**
   * Increment a specific counter.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   */
  void incrementCounter(Statistic op, long count);

  /**
   * Increment a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  void incrementGauge(Statistic op, long count);

  /**
   * Decrement a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  void decrementGauge(Statistic op, long count);

  /**
   * Add a value to a quantiles statistic. No-op if the quantile
   * isn't found.
   * @param op operation to look up.
   * @param value value to add.
   * @throws ClassCastException if the metric is not a Quantiles.
   */
  void addValueToQuantiles(Statistic op, long value);

  /**
   * Record a duration.
   * @param op operation
   * @param success was the operation a success?
   * @param duration how long did it take
   */
  void recordDuration(Statistic op, boolean success, Duration duration);
}
