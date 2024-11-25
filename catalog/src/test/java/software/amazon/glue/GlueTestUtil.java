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
package software.amazon.glue;

import static org.apache.iceberg.types.Types.NestedField.required;
import static software.amazon.glue.GlueLoadTableConfig.SERVER_SIDE_SCAN_PLANNING_ENABLED;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_ACCESS_KEY_ID;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_DATA_TRANSFER_ROLE_ARN;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_EXPIRATION_MS;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_LOCATION;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_SECRET_ACCESS_KEY;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_SESSION_TOKEN;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GlueFileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PlannedCachingScanTaskIterable;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.mockserver.integration.ClientAndServer;

public class GlueTestUtil {

  public static final GlueExtensionsProperties TEST_PROPERTIES =
      new GlueExtensionsProperties(ImmutableMap.of());

  public static final SessionCatalog.SessionContext TEST_SESSION_CONTEXT =
      new SessionCatalog.SessionContext(
          "id",
          "user",
          ImmutableMap.of(
              GlueExtensionsSessionProperties.CREDENTIALS_AWS_ACCESS_KEY_ID, "access-key-id",
              GlueExtensionsSessionProperties.CREDENTIALS_AWS_SECRET_ACCESS_KEY,
                  "secret-access-key",
              GlueExtensionsSessionProperties.CREDENTIALS_AWS_SESSION_TOKEN, "session-token"),
          ImmutableMap.of());

  public static final GlueExtensionsSessionProperties TEST_SESSION_PROPERTIES =
      new GlueExtensionsSessionProperties(TEST_SESSION_CONTEXT);

  public static final GlueExtensionsEndpoint TEST_ENDPOINT =
      new GlueExtensionsEndpoint("http://localhost:1234", "us-east-1");

  public static final Schema TEST_SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  public static final Schema TEST_SCHEMA_WITH_METADATA_COLUMNS =
      new Schema(
          ImmutableList.<NestedField>builder()
              .addAll(TEST_SCHEMA.columns())
              .add(MetadataColumns.FILE_PATH)
              .add(MetadataColumns.ROW_POSITION)
              .build());

  public static final PartitionSpec TEST_SPEC =
      PartitionSpec.builderFor(TEST_SCHEMA).bucket("data", 16).build();

  public static final GlueExtensionsPaths TEST_PATHS = new GlueExtensionsPaths(":");
  public static final TableMetadata TABLE_METADATA =
      TableMetadata.newTableMetadata(TEST_SCHEMA, TEST_SPEC, "s3://foo", ImmutableMap.of());

  public static GlueExtensionsEndpoint mockServerEndpoint(ClientAndServer mockServer) {
    return new GlueExtensionsEndpoint("http://localhost:" + mockServer.getPort(), "us-east-1");
  }

  public static String readResourceFile(String fileName) {
    try {
      return IOUtils.toString(GlueTestUtil.class.getResourceAsStream("/" + fileName));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static DataFile dataFile(String id) {
    return DataFiles.builder(TEST_SPEC)
        .withPath(String.format("/path/to/data-%s.parquet", id))
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0")
        .withRecordCount(1)
        .build();
  }

  public static DeleteFile deleteFile(String id) {
    return FileMetadata.deleteFileBuilder(TEST_SPEC)
        .ofPositionDeletes()
        .withPath(String.format("/path/to/data-%s-deletes.parquet", id))
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0")
        .withRecordCount(1)
        .build();
  }

  public static FileScanTask createTestFileScanTask() {
    return createFileScanTask(TEST_SPEC, "a", Lists.newArrayList("a", "a2"));
  }

  public static FileScanTask createTestFileScanTask(String id) {
    return createFileScanTask(TEST_SPEC, id, Lists.newArrayList(id, id + "-2"));
  }

  public static FileScanTask createFileScanTask(
      PartitionSpec spec, String dataFileId, List<String> deleteFileIds) {
    ResidualEvaluator residualEvaluator;
    if (spec.isUnpartitioned()) {
      residualEvaluator = ResidualEvaluator.unpartitioned(Expressions.alwaysTrue());
    } else {
      residualEvaluator = ResidualEvaluator.of(spec, Expressions.equal("id", 1), false);
    }
    return new BaseFileScanTask(
        dataFile(dataFileId),
        deleteFileIds.stream().map(GlueTestUtil::deleteFile).toArray(DeleteFile[]::new),
        SchemaParser.toJson(TEST_SCHEMA),
        PartitionSpecParser.toJson(spec),
        residualEvaluator);
  }

  public static List<String> toSerializedFileScanTasks(Iterable<FileScanTask> tasks) {
    return Streams.stream(tasks)
        .map(
            t -> {
              if (t instanceof GlueFileScanTask) {
                return ((GlueFileScanTask) t).delegate();
              }
              return t;
            })
        .map(ScanTaskParser::toJson)
        .collect(Collectors.toList());
  }

  public static GlueLoadTableConfig glueLoadTableConfig() {
    return glueLoadTableConfig(Clock.systemUTC());
  }

  public static GlueLoadTableConfig glueLoadTableConfig(Clock clock) {
    return new GlueLoadTableConfig(
        ImmutableMap.<String, String>builder()
            .put(SERVER_SIDE_SCAN_PLANNING_ENABLED, "true")
            .put(STAGING_LOCATION, "staging-location")
            .put(STAGING_ACCESS_KEY_ID, "test-access-key-id")
            .put(STAGING_SECRET_ACCESS_KEY, "test-secret-access-key")
            .put(STAGING_SESSION_TOKEN, "test-session-token")
            .put(
                STAGING_EXPIRATION_MS,
                Long.toString(Instant.now(clock).plus(Duration.ofHours(1)).toEpochMilli()))
            .put(STAGING_DATA_TRANSFER_ROLE_ARN, "test-data-transfer-role-arn")
            .build());
  }

  public static Expression bindExpression(Schema schema, Expression child) {
    return Binder.bind(schema.asStruct(), child, false);
  }

  public static PlannedCachingScanTaskIterable createCachedScanTaskIterable(FileScanTask... tasks) {
    PlannedCachingScanTaskIterable scanTask =
        new PlannedCachingScanTaskIterable(
            CloseableIterable.withNoopClose(ImmutableList.copyOf(tasks)));
    return scanTask;
  }
}
