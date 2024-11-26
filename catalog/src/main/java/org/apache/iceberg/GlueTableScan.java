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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import software.amazon.glue.GlueExtensionsTableOperations;
import software.amazon.glue.requests.ImmutablePlanTableRequest;
import software.amazon.glue.requests.ImmutablePreplanTableRequest;
import software.amazon.glue.requests.PreplanTableRequest;
import software.amazon.glue.responses.PlanTableIterable;
import software.amazon.glue.responses.PreplanTableIterable;

public class GlueTableScan extends DataTableScan {

  public GlueTableScan(GlueTable table, Schema schema, TableScanContext tableScanContext) {
    super(table, schema, tableScanContext);
  }

  private GlueTable glueTable() {
    return (GlueTable) table();
  }

  private GlueExtensionsTableOperations glueTableOps() {
    return (GlueExtensionsTableOperations) glueTable().operations();
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    if (!glueTable().isFileScanTaskCachingEnabled()) {
      return fetchFileScanTasks();
    }

    Expression boundedFilterExpression = Binder.bind(schema().asStruct(), filter(), false);
    for (Map.Entry<ScanCacheKey, PlannedCachingScanTaskIterable> entry :
        glueTable().getScanCache().asMap().entrySet()) {
      ScanCacheKey cacheKey = entry.getKey();
      if (cacheKey.canReuseScanCache(boundedFilterExpression, schema())) {
        return entry.getValue();
      }
    }
    return createCachedScan(boundedFilterExpression);
  }

  @Override
  protected TableScan newRefinedScan(
      Table refinedTable, Schema refinedSchema, TableScanContext refinedContext) {
    return new GlueTableScan((GlueTable) refinedTable, refinedSchema, refinedContext);
  }

  private CloseableIterable<FileScanTask> fetchFileScanTasks() {
    List<String> columnNames =
        schema().columns().stream()
            .filter(column -> !MetadataColumns.isMetadataColumn(column.fieldId()))
            .map(Types.NestedField::name)
            .collect(Collectors.toList());
    PreplanTableRequest request =
        ImmutablePreplanTableRequest.builder()
            .select(columnNames)
            .filter(ExpressionParser.toJson(filter()))
            .build();

    CloseableIterable<String> shards =
        new PreplanTableIterable(
            request,
            glueTableOps().client(),
            glueTableOps().paths().preplan(glueTableOps().identifier()),
            glueTableOps().properties());

    Iterable<CloseableIterable<FileScanTask>> tasks =
        Iterables.transform(
            shards,
            shard ->
                new PlanTableIterable(
                    ImmutablePlanTableRequest.builder().shard(shard).build(),
                    glueTableOps().client(),
                    glueTableOps().paths().plan(glueTableOps().identifier()),
                    glueTableOps().properties()));
    return CloseableIterable.concat(tasks);
  }

  private PlannedCachingScanTaskIterable createCachedScan(Expression filter) {
    CloseableIterable<FileScanTask> scanTasksIterable = fetchFileScanTasks();
    PlannedCachingScanTaskIterable cachingScanTask =
        new PlannedCachingScanTaskIterable(scanTasksIterable);
    ScanCacheKey cacheKey = new ScanCacheKey(filter, schema());
    glueTable().getScanCache().put(cacheKey, cachingScanTask);
    return cachingScanTask;
  }
}
