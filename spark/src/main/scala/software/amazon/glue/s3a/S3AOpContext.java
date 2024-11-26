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

import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import software.amazon.glue.s3a.impl.ActiveOperationContext;
import software.amazon.glue.s3a.statistics.S3AStatisticsContext;

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

  final Invoker invoker;
  @Nullable final FileSystem.Statistics stats;

  /** FileStatus for "destination" path being operated on. */
  protected final FileStatus dstFileStatus;

  /**
   * Constructor.
   * @param invoker invoker, which contains retry policy
   * @param stats optional stats object
   * @param instrumentation instrumentation to use
   * @param dstFileStatus file status from existence check
   */
  public S3AOpContext(Invoker invoker,
      @Nullable FileSystem.Statistics stats,
      S3AStatisticsContext instrumentation,
      FileStatus dstFileStatus) {

    super(newOperationId(),
        instrumentation
    );
    Preconditions.checkNotNull(invoker, "Null invoker arg");
    Preconditions.checkNotNull(instrumentation, "Null instrumentation arg");
    Preconditions.checkNotNull(dstFileStatus, "Null dstFileStatus arg");

    this.invoker = invoker;
    this.stats = stats;
    this.dstFileStatus = dstFileStatus;
  }

  public Invoker getInvoker() {
    return invoker;
  }

  @Nullable
  public FileSystem.Statistics getStats() {
    return stats;
  }

  public FileStatus getDstFileStatus() {
    return dstFileStatus;
  }
}
