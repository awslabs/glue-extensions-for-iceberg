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

import static org.apache.iceberg.BaseMetastoreCatalog.fullTableName;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.glue.GlueExtensionsTableOperations;
import software.amazon.glue.GlueTableLocationProvider;
import software.amazon.glue.GlueTableProperties;
import software.amazon.glue.operations.DataFileManifestGenerator;
import software.amazon.glue.operations.GlueAppendFiles;
import software.amazon.glue.operations.GlueDeleteFiles;
import software.amazon.glue.operations.GlueOverwriteFiles;
import software.amazon.glue.operations.GlueTransaction;
import software.amazon.glue.operations.IcebergJsonDataManifestGenerator;
import software.amazon.glue.operations.ManifestType;
import software.amazon.glue.operations.RedshiftFileManifestGenerator;

/**
 * A {@link org.apache.iceberg.BaseTable} that uses Glue extensions APIs for read and write
 * operations
 */
public class GlueTable extends BaseTable {

  private final GlueExtensionsTableOperations ops;
  private final DataFileManifestGenerator manifestBuilder;
  private final Cache<ScanCacheKey, PlannedCachingScanTaskIterable> scanCache;

  private Map<String, String> glueColumnMapping;
  private TableOperations originalOps;

  private volatile BaseTable baseTable;

  public GlueTable(GlueExtensionsTableOperations ops, String name, MetricsReporter reporter) {
    super(ops, name, reporter);
    this.ops = ops;
    this.scanCache =
        Caffeine.newBuilder()
            .expireAfterAccess(
                ops.properties().scanPlanningCacheExpirationMs(), TimeUnit.MILLISECONDS)
            .build();
    ManifestType manifestType = ManifestType.fromName(ops.properties().dataCommitManifestType());
    switch (manifestType) {
      case ICEBERG_JSON:
        this.manifestBuilder = new IcebergJsonDataManifestGenerator(ops, specs());
        break;
      case REDSHIFT:
        this.manifestBuilder = new RedshiftFileManifestGenerator(ops);
        break;
      default:
        throw new IllegalArgumentException("Unsupported manifest type: " + manifestType);
    }
  }

  @Override
  public String location() {
    if (ops.properties().tableMetadataUseStagingLocation()
        && ops.loadTableConfig().stagingLocation() != null) {
      return ops.loadTableConfig().stagingLocation();
    }
    return super.location();
  }

  @Override
  public Map<String, String> properties() {
    // Get the current properties from the superclass
    Map<String, String> properties = Maps.newHashMap(super.properties());

    // Use computeIfAbsent to add the NameMapping as a new key-value pair only if it's absent
    properties.computeIfAbsent(
        TableProperties.DEFAULT_NAME_MAPPING,
        key ->
            NameMappingParser.toJson(
                NameMapping.of(
                    this.schema().columns().stream()
                        .map(f -> MappedField.of(f.fieldId(), f.name()))
                        .collect(Collectors.toList()))));

    // force the following properties
    properties.put(TableProperties.DATA_PLANNING_MODE, PlanningMode.LOCAL.modeName());
    properties.put(TableProperties.DELETE_PLANNING_MODE, PlanningMode.LOCAL.modeName());

    if (ops.properties().tableMetadataUseStagingLocation()
        && ops.loadTableConfig().stagingLocation() != null) {
      properties.put(TableProperties.OBJECT_STORE_ENABLED, "true");
    } else {
      properties.put(
          TableProperties.WRITE_LOCATION_PROVIDER_IMPL, GlueTableLocationProvider.class.getName());
      properties.put(GlueTableProperties.WRITE_LOCATION_PROVIDER_REST_TABLE_PATH, ops.tablePath());
    }

    if (ops.loadTableConfig().stagingLocation() != null) {
      properties.put(
          GlueTableProperties.WRITE_LOCATION_PROVIDER_STAGING_LOCATION,
          ops.loadTableConfig().stagingLocation());
    }

    return properties;
  }

  public Map<String, String> glueColumnMapping() {
    return glueColumnMapping;
  }

  public void setGlueColumnTypeMappingFromGlueServiceTable(Table glueServiceTable) {
    if (glueServiceTable != null
        && glueServiceTable.storageDescriptor() != null
        && glueServiceTable.storageDescriptor().columns() != null) {
      this.glueColumnMapping =
          glueServiceTable.storageDescriptor().columns().stream()
              .collect(Collectors.toMap(Column::name, Column::type));
    }
  }

  public void setOriginalOps(TableOperations originalOps) {
    this.originalOps = originalOps;
  }

  public BaseTable baseTable() {
    if (originalOps != null && baseTable == null) {
      synchronized (this) {
        if (baseTable == null) {
          baseTable =
              new BaseTable(originalOps, fullTableName(name(), ops.identifier()), reporter());
        }
      }
    }
    return baseTable;
  }

  public boolean isFileScanTaskCachingEnabled() {
    return ops.properties().tableCacheFileScanTasks();
  }

  public String redshiftTableIdentifier() {
    return ops.redshiftTableIdentifier();
  }

  public boolean serverSideScanPlanningEnabled() {
    return ops.loadTableConfig().serverSideScanPlanningEnabled();
  }

  public boolean serverSideDataCommitEnabled() {
    return ops.loadTableConfig().serverSideDataCommitEnabled();
  }

  @Override
  public TableScan newScan() {
    if (serverSideScanPlanningEnabled()) {
      return new GlueTableScan(
          this, schema(), ImmutableTableScanContext.builder().metricsReporter(reporter()).build());
    }
    if (originalOps != null) {
      return baseTable().newScan();
    }
    return super.newScan();
  }

  @Override
  public AppendFiles newAppend() {
    if (serverSideDataCommitEnabled()) {
      return new GlueAppendFiles(this);
    }
    if (originalOps != null) {
      return baseTable().newAppend();
    }
    return super.newAppend();
  }

  @Override
  public DeleteFiles newDelete() {
    if (serverSideDataCommitEnabled()) {
      return new GlueDeleteFiles(this);
    }
    if (originalOps != null) {
      return baseTable().newDelete();
    }
    return super.newDelete();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    if (serverSideDataCommitEnabled()) {
      return new GlueOverwriteFiles(this);
    }
    if (originalOps != null) {
      return baseTable().newOverwrite();
    }
    return super.newOverwrite();
  }

  public DataFileManifestGenerator manifestBuilder() {
    return manifestBuilder;
  }

  public Cache<ScanCacheKey, PlannedCachingScanTaskIterable> getScanCache() {
    return scanCache;
  }

  public void invalidateScanCache() {
    scanCache.invalidateAll();
  }

  @Override
  public Transaction newTransaction() {
    if (serverSideDataCommitEnabled()) {
      return new GlueTransaction(this, super.newTransaction());
    }
    if (originalOps != null) {
      return baseTable().newTransaction();
    }
    return super.newTransaction();
  }
}
