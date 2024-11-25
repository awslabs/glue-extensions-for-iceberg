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
package software.amazon.glue.operations;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ScanCacheKey;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;

public class GlueOverwriteFiles extends GlueUpdateFilesOperation<OverwriteFiles>
    implements OverwriteFiles {

  private static final int AVERAGE_COLUMN_SIZE = 40;
  private static final int SQL_BUFFER_SIZE = 200;
  private static final int MAX_SQL_SIZE = 10240;

  public GlueOverwriteFiles(GlueTable table) {
    super(table);
  }

  @Override
  protected OverwriteFiles self() {
    return this;
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expr) {
    // no-op SparkWrite operation will call to set overwrite by row filter
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    return add(file);
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
    return remove(file);
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    // no-op SparkWrite operation will call to validate conflicting deletes
    return this;
  }

  @Override
  public OverwriteFiles validateFromSnapshot(long snapshotId) {
    // no-op SparkWrite operation will set snapshotId
    return this;
  }

  @Override
  public OverwriteFiles caseSensitive(boolean caseSensitive) {
    throw new UnsupportedOperationException(
        "Setting case sensitivity for overwrite operation is not supported");
  }

  @Override
  public OverwriteFiles conflictDetectionFilter(Expression conflictDetectionFilter) {
    // no-op SparkWrite operation will set conflict detection filter
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingData() {
    // no-op SparkWrite operation will call to validate conflicting data
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingDeletes() {
    // no-op SparkWrite operation will call to validate conflicting deletes
    return this;
  }

  @Override
  public void commit() {
    if (hasChanges() && canApplyDeleteFilter()) {
      applyDeleteFilterOrFallback();
    } else {
      super.commit();
    }
  }

  private boolean canApplyDeleteFilter() {
    return schemaContainsDoubleOrFloat()
        || schemaExceedsMaximumSqlLimit()
        || deletedFilesMatchScan();
  }

  private void applyDeleteFilterOrFallback() {
    Map.Entry<ScanCacheKey, List<DataFile>> expressionAndScan = findMostSpecificMatchingScan();
    if (expressionAndScan != null) {
      overwriteWithDeleteFilter(expressionAndScan);
    } else {
      super.commit();
    }
  }

  private boolean schemaContainsDoubleOrFloat() {
    return table.schema().columns().stream()
        .anyMatch(
            column ->
                column.type() instanceof Types.DoubleType
                    || column.type() instanceof Types.FloatType);
  }

  private boolean deletedFilesMatchScan() {
    return table.getScanCache().asMap().values().stream()
        .anyMatch(scanTask -> scanTask.getDataFilePaths().containsAll(deletedDataFilePaths));
  }

  private boolean schemaExceedsMaximumSqlLimit() {
    return ((table.schema().columns().size() * AVERAGE_COLUMN_SIZE) * 2) + SQL_BUFFER_SIZE
        > MAX_SQL_SIZE;
  }

  private Map.Entry<ScanCacheKey, List<DataFile>> findMostSpecificMatchingScan() {
    return table.getScanCache().asMap().entrySet().stream()
        .filter(entry -> entry.getValue().isMaterialized())
        .filter(entry -> entry.getValue().getDataFilePaths().containsAll(deletedDataFilePaths))
        .min(Comparator.comparingInt(entry -> entry.getValue().getDataFilePaths().size()))
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().getCachedResults().stream()
                        .map(FileScanTask::file)
                        .collect(Collectors.toList())))
        .orElse(null);
  }

  private void overwriteWithDeleteFilter(
      Map.Entry<ScanCacheKey, List<DataFile>> expressionAndScan) {
    expressionAndScan.getValue().stream()
        .filter(file -> !deletedDataFilePaths.contains(file.path().toString()))
        .forEach(this::add);
    commitWithDeleteFilterRequest(expressionAndScan.getKey().getExpression());
  }
}
