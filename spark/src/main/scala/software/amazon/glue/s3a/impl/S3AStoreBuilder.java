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

import org.apache.hadoop.fs.FileSystem;
import software.amazon.glue.s3a.S3AInstrumentation;
import software.amazon.glue.s3a.S3AStorageStatistics;
import software.amazon.glue.s3a.S3AStore;
import software.amazon.glue.s3a.audit.AuditSpanS3A;
import software.amazon.glue.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.util.RateLimiting;

/**
 * Builder for the S3AStore.
 */
public class S3AStoreBuilder {

  private StoreContextFactory storeContextFactory;

  private ClientManager clientManager;

  private DurationTrackerFactory durationTrackerFactory;

  private S3AInstrumentation instrumentation;

  private S3AStatisticsContext statisticsContext;

  private S3AStorageStatistics storageStatistics;

  private RateLimiting readRateLimiter;

  private RateLimiting writeRateLimiter;

  private AuditSpanSource<AuditSpanS3A> auditSpanSource;

  /**
   * The original file system statistics: fairly minimal but broadly
   * collected so it is important to pick up.
   * This may be null.
   */
  private FileSystem.Statistics fsStatistics;

  public S3AStoreBuilder withStoreContextFactory(
          final StoreContextFactory storeContextFactoryValue) {
    this.storeContextFactory = storeContextFactoryValue;
    return this;
  }

  public S3AStoreBuilder withClientManager(
          final ClientManager manager) {
    this.clientManager = manager;
    return this;
  }

  public S3AStoreBuilder withDurationTrackerFactory(
          final DurationTrackerFactory durationTrackerFactoryValue) {
    this.durationTrackerFactory = durationTrackerFactoryValue;
    return this;
  }

  public S3AStoreBuilder withInstrumentation(
          final S3AInstrumentation instrumentationValue) {
    this.instrumentation = instrumentationValue;
    return this;
  }

  public S3AStoreBuilder withStatisticsContext(
          final S3AStatisticsContext statisticsContextValue) {
    this.statisticsContext = statisticsContextValue;
    return this;
  }

  public S3AStoreBuilder withStorageStatistics(
          final S3AStorageStatistics storageStatisticsValue) {
    this.storageStatistics = storageStatisticsValue;
    return this;
  }

  public S3AStoreBuilder withReadRateLimiter(
          final RateLimiting readRateLimiterValue) {
    this.readRateLimiter = readRateLimiterValue;
    return this;
  }

  public S3AStoreBuilder withWriteRateLimiter(
          final RateLimiting writeRateLimiterValue) {
    this.writeRateLimiter = writeRateLimiterValue;
    return this;
  }

  public S3AStoreBuilder withAuditSpanSource(
          final AuditSpanSource<AuditSpanS3A> auditSpanSourceValue) {
    this.auditSpanSource = auditSpanSourceValue;
    return this;
  }

  public S3AStoreBuilder withFsStatistics(final FileSystem.Statistics value) {
    this.fsStatistics = value;
    return this;
  }

  public S3AStore build() {
    return new S3AStoreImpl(storeContextFactory,
        clientManager,
        durationTrackerFactory,
        instrumentation,
        statisticsContext,
        storageStatistics,
        readRateLimiter,
        writeRateLimiter,
        auditSpanSource,
        fsStatistics);
  }
}
