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

import javax.annotation.Nullable;

import java.time.Duration;

import org.apache.hadoop.fs.FileSystem;
import software.amazon.glue.s3a.S3AInstrumentation;
import software.amazon.glue.s3a.Statistic;
import software.amazon.glue.s3a.statistics.BlockOutputStreamStatistics;
import software.amazon.glue.s3a.statistics.CommitterStatistics;
import software.amazon.glue.s3a.statistics.DelegationTokenStatistics;
import software.amazon.glue.s3a.statistics.S3AInputStreamStatistics;
import software.amazon.glue.s3a.statistics.S3AMultipartUploaderStatistics;
import software.amazon.glue.s3a.statistics.S3AStatisticsContext;
import software.amazon.glue.s3a.statistics.StatisticsFromAwsSdk;
import org.apache.hadoop.fs.statistics.DurationTracker;

/**
 * An S3A statistics context which is bonded to a
 * S3AInstrumentation instance -inevitably that of an S3AFileSystem
 * instance.
 * <p>
 * An interface is used to bind to the relevant fields, rather
 * than have them passed in the constructor because some
 * production code, specifically, DelegateToFileSystem,
 * patches the protected field after initialization.
 * </p>
 * <p>
 * All operations are passed through directly to that class.
 * </p>
 * <p>
 * If an instance of FileSystem.Statistics is passed in, it
 * will be used whenever input stream statistics are created -
 * However, Internally always increments the statistics in the
 * current thread.
 * </p>
 * <p>
 * As a result, cross-thread IO will under-report.
 * </p>
 *
 * This is addressed through the stream statistics classes
 * only updating the stats in the close() call. Provided
 * they are closed in the worker thread, all stats collected in
 * helper threads will be included.
 */
public class BondedS3AStatisticsContext implements S3AStatisticsContext {

  /** Source of statistics services. */
  private final S3AFSStatisticsSource statisticsSource;

  /**
   * Instantiate.
   * @param statisticsSource integration binding
   */
  public BondedS3AStatisticsContext(
      final S3AFSStatisticsSource statisticsSource) {
    this.statisticsSource = statisticsSource;
  }


  /**
   * Get the instrumentation from the FS integration.
   * @return instrumentation instance.
   */
  private S3AInstrumentation getInstrumentation() {
    return statisticsSource.getInstrumentation();
  }

  /**
   * The filesystem statistics: know this is thread-local.
   * @return FS statistics.
   */
  private FileSystem.Statistics getInstanceStatistics() {
    return statisticsSource.getInstanceStatistics();
  }

  /**
   * Create a stream input statistics instance.
   * The FileSystem.Statistics instance of the {@link #statisticsSource}
   * is used as the reference to FileSystem statistics to update
   * @return the new instance
   */
  @Override
  public S3AInputStreamStatistics newInputStreamStatistics() {
    return getInstrumentation().newInputStreamStatistics(
        statisticsSource.getInstanceStatistics());
  }

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  @Override
  public CommitterStatistics newCommitterStatistics() {
    return getInstrumentation().newCommitterStatistics();
  }

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  @Override
  public BlockOutputStreamStatistics newOutputStreamStatistics() {
    return getInstrumentation()
        .newOutputStreamStatistics(getInstanceStatistics());
  }

  /**
   * Increment a specific counter.
   * <p>
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   */
  @Override
  public void incrementCounter(Statistic op, long count) {
    getInstrumentation().incrementCounter(op, count);
  }

  /**
   * Increment a specific gauge.
   * <p>
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  @Override
  public void incrementGauge(Statistic op, long count) {
    getInstrumentation().incrementGauge(op, count);
  }

  /**
   * Decrement a specific gauge.
   * <p>
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  @Override
  public void decrementGauge(Statistic op, long count) {
    getInstrumentation().decrementGauge(op, count);
  }

  /**
   * Add a value to a quantiles statistic. No-op if the quantile
   * isn't found.
   * @param op operation to look up.
   * @param value value to add.
   * @throws ClassCastException if the metric is not a Quantiles.
   */
  @Override
  public void addValueToQuantiles(Statistic op, long value) {
    getInstrumentation().addValueToQuantiles(op, value);
  }

  @Override
  public void recordDuration(final Statistic op,
      final boolean success,
      final Duration duration) {
    getInstrumentation().recordDuration(op, success, duration);
  }

  /**
   * Create a delegation token statistics instance.
   * @return an instance of delegation token statistics
   */
  @Override
  public DelegationTokenStatistics newDelegationTokenStatistics() {
    return getInstrumentation().newDelegationTokenStatistics();
  }

  @Override
  public StatisticsFromAwsSdk newStatisticsFromAwsSdk() {
    return new StatisticsFromAwsSdkImpl(getInstrumentation());
  }

  @Override
  public S3AMultipartUploaderStatistics createMultipartUploaderStatistics() {
    return new S3AMultipartUploaderStatisticsImpl(this::incrementCounter);
  }

  @Override
  public DurationTracker trackDuration(final String key, final long count) {
    return getInstrumentation().trackDuration(key, count);
  }

  /**
   * This is the interface which an integration source must implement
   * for the integration.
   * Note that the FileSystem.statistics field may be null for a class;
   */
  public interface S3AFSStatisticsSource {

    /**
     * Get the S3A Instrumentation.
     * @return a non-null instrumentation instance
     */
    S3AInstrumentation getInstrumentation();

    /**
     * Get the statistics of the FS instance, shared across all threads.
     * @return filesystem statistics
     */
    @Nullable
    FileSystem.Statistics getInstanceStatistics();

  }
}
