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

import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import software.amazon.glue.s3a.impl.ActiveOperationContext;
import software.amazon.glue.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Class for operation context struct passed through codepaths for main
 * S3AFileSystem operations.
 * Anything op-specific should be moved to a subclass of this.
 *
 * This was originally a base class, but {@link ActiveOperationContext} was
 * created to be more minimal and cover many more operation type.
 */
@SuppressWarnings("visibilitymodifier")
public class S3AOpContext extends ActiveOperationContext {

  final boolean isS3GuardEnabled;
  final Invoker invoker;
  @Nullable final FileSystem.Statistics stats;
  @Nullable final Invoker s3guardInvoker;

  /** FileStatus for "destination" path being operated on. */
  protected final FileStatus dstFileStatus;

  /**
   * Alternate constructor that allows passing in two invokers, the common
   * one, and another with the S3Guard Retry Policy.
   * @param isS3GuardEnabled true if s3Guard is active
   * @param invoker invoker, which contains retry policy
   * @param s3guardInvoker s3guard-specific retry policy invoker
   * @param stats optional stats object
   * @param instrumentation instrumentation to use
   * @param dstFileStatus file status from existence check
   */
  public S3AOpContext(boolean isS3GuardEnabled, Invoker invoker,
      @Nullable Invoker s3guardInvoker,
      @Nullable FileSystem.Statistics stats,
      S3AStatisticsContext instrumentation,
      FileStatus dstFileStatus) {

    super(newOperationId(),
        instrumentation,
        null);
    Preconditions.checkNotNull(invoker, "Null invoker arg");
    Preconditions.checkNotNull(instrumentation, "Null instrumentation arg");
    Preconditions.checkNotNull(dstFileStatus, "Null dstFileStatus arg");
    this.isS3GuardEnabled = isS3GuardEnabled;
    Preconditions.checkArgument(!isS3GuardEnabled || s3guardInvoker != null,
        "S3Guard invoker required: S3Guard is enabled.");
    this.invoker = invoker;
    this.s3guardInvoker = s3guardInvoker;
    this.stats = stats;
    this.dstFileStatus = dstFileStatus;
  }

  /**
   * Constructor using common invoker and retry policy.
   * @param isS3GuardEnabled true if s3Guard is active
   * @param invoker invoker, which contains retry policy
   * @param stats optional stats object
   * @param instrumentation instrumentation to use
   * @param dstFileStatus file status from existence check
   */
  public S3AOpContext(boolean isS3GuardEnabled,
      Invoker invoker,
      @Nullable FileSystem.Statistics stats,
      S3AStatisticsContext instrumentation,
      FileStatus dstFileStatus) {
    this(isS3GuardEnabled, invoker, null, stats, instrumentation,
        dstFileStatus);
  }

  public boolean isS3GuardEnabled() {
    return isS3GuardEnabled;
  }

  public Invoker getInvoker() {
    return invoker;
  }

  @Nullable
  public FileSystem.Statistics getStats() {
    return stats;
  }

  @Nullable
  public Invoker getS3guardInvoker() {
    return s3guardInvoker;
  }

  public FileStatus getDstFileStatus() {
    return dstFileStatus;
  }
}
