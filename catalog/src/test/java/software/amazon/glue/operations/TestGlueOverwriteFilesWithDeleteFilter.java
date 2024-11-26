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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PlannedCachingScanTaskIterable;
import org.apache.iceberg.ScanCacheKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.glue.GlueExtensionsTableOperations;
import software.amazon.glue.GlueTestUtil;

public class TestGlueOverwriteFilesWithDeleteFilter {
  private static final Schema FLOAT_SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.FloatType.get()));

  private GlueOverwriteFiles glueOverwriteFiles;
  private PlannedCachingScanTaskIterable mockCachedScanTask;

  @BeforeEach
  public void setUp() {
    glueOverwriteFiles = Mockito.spy(createMockOverwriteFiles());
    mockCachedScanTask = Mockito.mock(PlannedCachingScanTaskIterable.class);
  }

  private GlueOverwriteFiles createMockOverwriteFiles() {
    GlueTable glueTable = Mockito.mock(GlueTable.class);
    Cache<Expression, PlannedCachingScanTaskIterable> mockScanCache = Mockito.mock(Cache.class);
    DataFileManifestGenerator mockManifestGenerator = Mockito.mock(DataFileManifestGenerator.class);
    GlueExtensionsTableOperations tableOperations =
        Mockito.mock(GlueExtensionsTableOperations.class);

    Mockito.doReturn(new ConcurrentHashMap<>()).when(mockScanCache).asMap();
    Mockito.doReturn(mockScanCache).when(glueTable).getScanCache();
    Mockito.doReturn(tableOperations).when(glueTable).operations();
    Mockito.doReturn(GlueTestUtil.TEST_SCHEMA).when(glueTable).schema();
    Mockito.doReturn(GlueTestUtil.TABLE_METADATA).when(tableOperations).current();
    Mockito.doReturn(ImmutableList.of(GlueTestUtil.dataFile("a")))
        .when(mockManifestGenerator)
        .buildDataManifest(any());
    Mockito.doReturn(mockManifestGenerator).when(glueTable).manifestBuilder();
    Mockito.doNothing().when(tableOperations).commitUpdateWithTransaction(any(), any());
    return new GlueOverwriteFiles(glueTable);
  }

  @Test
  public void testDoubleSchemaTriggersDeleteFilter() {
    DataFile dataFile = GlueTestUtil.dataFile("a");
    Expression expression = Expressions.alwaysTrue();
    setUpScanCache(expression, dataFile);
    Mockito.doReturn(FLOAT_SCHEMA).when(glueOverwriteFiles.table).schema();

    glueOverwriteFiles.deleteFile(dataFile);
    glueOverwriteFiles.commit();

    validateCommitUpdateRequest(expression);
  }

  @Test
  public void testDeletedFilesMatchScanTriggersDeleteFilter() {
    DataFile dataFile = GlueTestUtil.dataFile("a");
    Expression expression = Expressions.alwaysTrue();
    setUpScanCache(expression, dataFile);

    glueOverwriteFiles.deleteFile(dataFile);
    glueOverwriteFiles.commit();

    validateCommitUpdateRequest(expression);
  }

  @Test
  public void testSchemaExceedsMaximumSqlLimitTriggersDeleteFilter() {
    DataFile dataFile = GlueTestUtil.dataFile("a");
    Expression expression = Expressions.alwaysTrue();
    setUpScanCache(expression, dataFile);
    Mockito.doReturn(createLargeSchema()).when(glueOverwriteFiles.table).schema();

    glueOverwriteFiles.deleteFile(dataFile);
    glueOverwriteFiles.commit();

    validateCommitUpdateRequest(expression);
  }

  @Test
  public void testFallbackToMetadataCommit() {
    DataFile dataFile = GlueTestUtil.dataFile("a");

    glueOverwriteFiles.deleteFile(dataFile);
    glueOverwriteFiles.commit();

    ArgumentCaptor<MetadataUpdate> requestCaptor = ArgumentCaptor.forClass(MetadataUpdate.class);
    Mockito.verify(glueOverwriteFiles, times(1)).commitWithTransaction(requestCaptor.capture());
    validateFallbackToMetadataCommit(requestCaptor.getValue());
  }

  @Test
  public void testNoFilesAddedOrRemoved() {
    glueOverwriteFiles.commit();

    Mockito.verify(glueOverwriteFiles, never()).commitWithTransaction(any());
    Mockito.verify(glueOverwriteFiles.table, never()).invalidateScanCache();
    Mockito.verify(glueOverwriteFiles.table.manifestBuilder(), never()).buildDataManifest(any());
  }

  @Test
  public void testNoFilesAddedOrRemovedWithMatchingScan() {
    DataFile dataFile = GlueTestUtil.dataFile("a");
    Expression expression = Expressions.alwaysTrue();
    setUpScanCache(expression, dataFile);
    Mockito.doReturn(createLargeSchema()).when(glueOverwriteFiles.table).schema();

    glueOverwriteFiles.commit();

    Mockito.verify(glueOverwriteFiles, never()).commitWithTransaction(any());
    Mockito.verify(glueOverwriteFiles.table, never()).invalidateScanCache();
    Mockito.verify(glueOverwriteFiles.table.manifestBuilder(), never()).buildDataManifest(any());
  }

  @Test
  public void testMostSpecificMatchingScanIsUsed() {
    FileScanTask scanTask1 = GlueTestUtil.createTestFileScanTask("a");
    FileScanTask scanTask2 = GlueTestUtil.createTestFileScanTask("b");
    FileScanTask scanTask3 = GlueTestUtil.createTestFileScanTask("c");
    Expression expression1 = Expressions.alwaysTrue();
    Expression expression2 = Expressions.greaterThan("column", new BigDecimal("3.14"));
    ScanCacheKey scanCacheKey1 = new ScanCacheKey(expression1, GlueTestUtil.TEST_SCHEMA);
    ScanCacheKey scanCacheKey2 = new ScanCacheKey(expression2, GlueTestUtil.TEST_SCHEMA);

    PlannedCachingScanTaskIterable largerScanTask =
        createMockScanTask(scanTask1, scanTask2, scanTask3);
    PlannedCachingScanTaskIterable smallerScanTask = createMockScanTask(scanTask1, scanTask2);

    ArgumentCaptor<MetadataUpdate> requestCaptor = ArgumentCaptor.forClass(MetadataUpdate.class);
    ArgumentCaptor<List<DataFile>> manifestCaptor = ArgumentCaptor.forClass(List.class);
    Cache<ScanCacheKey, PlannedCachingScanTaskIterable> mockScanCache =
        glueOverwriteFiles.table.getScanCache();
    Mockito.doReturn(
            new ConcurrentHashMap<>(
                ImmutableMap.of(
                    scanCacheKey1, largerScanTask,
                    scanCacheKey2, smallerScanTask)))
        .when(mockScanCache)
        .asMap();

    glueOverwriteFiles.deleteFile(scanTask1.file());
    glueOverwriteFiles.commit();

    Mockito.verify(glueOverwriteFiles.dataManifestBuilder, times(1))
        .buildDataManifest(manifestCaptor.capture());
    List<DataFile> capturedManifest = manifestCaptor.getValue();
    Assertions.assertThat(capturedManifest).hasSize(1);
    Assertions.assertThat(capturedManifest.get(0))
        .isEqualTo(scanTask2.file())
        .as(
            "DataFiles from the cached scan not marked for deletion should be added to the overwrite");
    Mockito.verify(glueOverwriteFiles, times(1)).commitWithTransaction(requestCaptor.capture());
    validateCommitUpdateRequest(expression2);
  }

  private void setUpScanCache(Expression expression, DataFile dataFile) {
    FileScanTask mockFileScanTask = Mockito.mock(FileScanTask.class);
    Cache<ScanCacheKey, PlannedCachingScanTaskIterable> mockScanCache =
        glueOverwriteFiles.table.getScanCache();
    ScanCacheKey scanCacheKey = new ScanCacheKey(expression, GlueTestUtil.TEST_SCHEMA);

    Mockito.doReturn(true).when(glueOverwriteFiles.table).isFileScanTaskCachingEnabled();
    Mockito.doReturn(dataFile).when(mockFileScanTask).file();
    Mockito.doReturn(true).when(mockCachedScanTask).isMaterialized();
    Mockito.doReturn(Sets.newHashSet(dataFile.path().toString()))
        .when(mockCachedScanTask)
        .getDataFilePaths();
    Mockito.doReturn(ImmutableList.of(mockFileScanTask))
        .when(mockCachedScanTask)
        .getCachedResults();
    Mockito.doReturn(new ConcurrentHashMap<>(ImmutableMap.of(scanCacheKey, mockCachedScanTask)))
        .when(mockScanCache)
        .asMap();
  }

  private PlannedCachingScanTaskIterable createMockScanTask(FileScanTask... tasks) {
    PlannedCachingScanTaskIterable scanTask = Mockito.mock(PlannedCachingScanTaskIterable.class);
    Mockito.doReturn(true).when(scanTask).isMaterialized();
    Mockito.doReturn(ImmutableList.copyOf(tasks)).when(scanTask).getCachedResults();
    Mockito.doReturn(
            Arrays.stream(tasks)
                .map(fileScanTask -> fileScanTask.file().path().toString())
                .collect(Collectors.toSet()))
        .when(scanTask)
        .getDataFilePaths();
    return scanTask;
  }

  private void validateCommitUpdateRequest(Expression expectedExpression) {
    ArgumentCaptor<MetadataUpdate> requestCaptor = ArgumentCaptor.forClass(MetadataUpdate.class);
    Mockito.verify(glueOverwriteFiles, times(1)).commitWithTransaction(requestCaptor.capture());

    MetadataUpdate request = requestCaptor.getValue();
    Assertions.assertThat(request).isNotNull();
    Assertions.assertThat(request).isInstanceOf(OverwriteRowsWithDeleteFilter.class);

    OverwriteRowsWithDeleteFilter update = (OverwriteRowsWithDeleteFilter) request;
    Assertions.assertThat(update.getDeleteFilter())
        .isEqualTo(ExpressionParser.toJson(expectedExpression));
    Mockito.verify(glueOverwriteFiles.table, times(1)).invalidateScanCache();
  }

  private void validateFallbackToMetadataCommit(MetadataUpdate request) {
    Assertions.assertThat(request).isNotNull();
    Assertions.assertThat(request).isInstanceOf(OverwriteRowsWithManifest.class);
  }

  private Schema createLargeSchema() {
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      fields.add(required(i, String.format("field_%d", i), Types.IntegerType.get()));
    }
    return new Schema(fields);
  }
}
