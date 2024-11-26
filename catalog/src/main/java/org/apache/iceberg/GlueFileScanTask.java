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
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;

public class GlueFileScanTask implements FileScanTask {

  private final FileScanTask delegate;

  private long deletesSizeBytes = 0;

  public GlueFileScanTask(FileScanTask delegate) {
    this.delegate = delegate;
  }

  public static FileScanTask from(String serializedFileScanTask) {
    return new GlueFileScanTask(
        ScanTaskParser.fromJson(serializedFileScanTask, false).asFileScanTask());
  }

  public FileScanTask delegate() {
    return delegate;
  }

  @Override
  public DataFile file() {
    return delegate.file();
  }

  @Override
  public List<DeleteFile> deletes() {
    return delegate.deletes();
  }

  @Override
  public Schema schema() {
    return delegate.schema();
  }

  @Override
  public PartitionSpec spec() {
    return delegate.spec();
  }

  @Override
  public long start() {
    return delegate.start();
  }

  @Override
  public long length() {
    return delegate.length();
  }

  @Override
  public long estimatedRowsCount() {
    return BaseContentScanTask.estimateRowsCount(length(), delegate.file());
  }

  @Override
  public long sizeBytes() {
    return length() + deletesSizeBytes();
  }

  @Override
  public int filesCount() {
    return delegate.filesCount();
  }

  @Override
  public Expression residual() {
    return delegate.residual();
  }

  @Override
  public Iterable<FileScanTask> split(long splitSize) {
    // TODO: actually create splits from the Parquet file scan task
    return FluentIterable.of(delegate);
  }

  private long deletesSizeBytes() {
    if (deletesSizeBytes == 0L && delegate.filesCount() > 1) {
      long size = 0L;
      for (DeleteFile deleteFile : delegate.deletes()) {
        size += deleteFile.fileSizeInBytes();
      }
      this.deletesSizeBytes = size;
    }

    return deletesSizeBytes;
  }
}
