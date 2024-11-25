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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueExtensionsPaths;
import software.amazon.glue.GlueExtensionsTableOperations;
import software.amazon.glue.GlueTestUtil;
import software.amazon.glue.requests.ImmutablePlanTableRequest;
import software.amazon.glue.requests.PlanTableRequestParser;
import software.amazon.glue.responses.ImmutablePlanTableResponse;
import software.amazon.glue.responses.ImmutablePreplanTableResponse;
import software.amazon.glue.responses.PlanTableResponseParser;
import software.amazon.glue.responses.PreplanTableResponseParser;

public class TestGlueTableScan {

  private static ClientAndServer mockServer;
  private static GlueExtensionsClient client;

  @BeforeAll
  public static void beforeClass() {
    mockServer = ClientAndServer.startClientAndServer();
    client =
        GlueExtensionsClient.builder()
            .withEndpoint(GlueTestUtil.mockServerEndpoint(mockServer))
            .withProperties(GlueTestUtil.TEST_PROPERTIES)
            .withSessionProperties(GlueTestUtil.TEST_SESSION_PROPERTIES)
            .build();
  }

  @AfterAll
  public static void afterClass() throws IOException {
    mockServer.stop();
    client.close();
  }

  @BeforeEach
  public void before() {
    mockServer.reset();
  }

  public GlueTable mockGlueTable() {
    GlueTable glueTable = Mockito.mock(GlueTable.class);
    GlueExtensionsTableOperations ops = Mockito.mock(GlueExtensionsTableOperations.class);
    Cache<ScanCacheKey, PlannedCachingScanTaskIterable> realCache =
        Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    Mockito.doReturn(realCache).when(glueTable).getScanCache();
    Mockito.doReturn(ops).when(glueTable).operations();
    Mockito.doReturn(client).when(ops).client();
    Mockito.doReturn(GlueTestUtil.TEST_PATHS).when(ops).paths();
    Mockito.doReturn(GlueTestUtil.TEST_PROPERTIES).when(ops).properties();
    Mockito.doReturn(GlueTestUtil.TEST_SESSION_PROPERTIES).when(ops).sessionProperties();
    Mockito.doReturn(TableIdentifier.of("ns", "tbl")).when(ops).identifier();
    return glueTable;
  }

  private GlueTableScan mockGlueTableScan() {
    GlueTable glueTable = mockGlueTable();
    return new GlueTableScan(
        glueTable, GlueTestUtil.TEST_SCHEMA_WITH_METADATA_COLUMNS, TableScanContext.empty());
  }

  @Test
  public void testSingleShardSingleTaskNoWait() {
    GlueTableScan glueTableScan = mockGlueTableScan();
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().addShards("shard1").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().addFileScanTasks(t1).build())));

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(glueTableScan.planFiles()))
        .isEqualTo(Lists.newArrayList(t1));
  }

  @Test
  public void testMultipleShardsMultipleTasksNoWait() {
    GlueTableScan glueTableScan = mockGlueTableScan();
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    String t3 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t3"));
    String t4 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t4"));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard1")
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().addShards("shard2").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard1").build())))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard2").build())))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t3)
                            .addFileScanTasks(t4)
                            .build())));

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(glueTableScan.planFiles()))
        .isEqualTo(Lists.newArrayList(t1, t2, t3, t4));
  }

  @Test
  public void testMultipleShardsMultipleTasksMultiplePagesNoWait() {
    GlueTableScan glueTableScan = mockGlueTableScan();
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    String t3 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t3"));
    String t4 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t4"));
    String t5 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t5"));
    String t6 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t6"));
    String t7 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t7"));
    String t8 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t8"));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard1")
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().addShards("shard2").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard1").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .nextPageToken("page1-1")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page1-1")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard1").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t3)
                            .addFileScanTasks(t4)
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard2").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t5)
                            .addFileScanTasks(t6)
                            .nextPageToken("page2-1")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2-1")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard2").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t7)
                            .addFileScanTasks(t8)
                            .build())));

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(glueTableScan.planFiles()))
        .containsAll(Lists.newArrayList(t1, t2, t3, t4, t5, t6, t7, t8));
  }

  @Test
  public void testMultipleShardsMultipleTasksMultiplePagesAndWait() {
    GlueTableScan glueTableScan = mockGlueTableScan();
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    String t3 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t3"));
    String t4 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t4"));
    String t5 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t5"));
    String t6 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t6"));
    String t7 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t7"));
    String t8 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t8"));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard1")
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().nextPageToken("page3").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page3"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().addShards("shard2").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard1").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .nextPageToken("page1-1")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page1-1")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard1").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().nextPageToken("page1-2").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page1-2")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard1").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t3)
                            .addFileScanTasks(t4)
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard2").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t5)
                            .addFileScanTasks(t6)
                            .nextPageToken("page2-1")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2-1")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard2").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().nextPageToken("page2-2").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2-2")
                .withBody(
                    PlanTableRequestParser.toJson(
                        ImmutablePlanTableRequest.builder().shard("shard2").build())),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t7)
                            .addFileScanTasks(t8)
                            .build())));

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(glueTableScan.planFiles()))
        .containsAll(Lists.newArrayList(t1, t2, t3, t4, t5, t6, t7, t8));
  }

  @Test
  public void testScanCacheReuseWithMatchingSchemaAndExpression() {
    FileScanTask t1 = GlueTestUtil.createTestFileScanTask("t1");
    FileScanTask t2 = GlueTestUtil.createTestFileScanTask("t2");
    GlueTableScan glueTableScan =
        mockGlueTableScanWithCache(Expressions.alwaysTrue(), GlueTestUtil.TEST_SCHEMA, t1, t2);

    CloseableIterable<FileScanTask> scanResult = glueTableScan.planFiles();
    Assertions.assertThat(scanResult).containsAll(Lists.newArrayList(t1, t2));
  }

  @Test
  public void testScanCacheReuseMissBySchema() {
    FileScanTask t1 = GlueTestUtil.createTestFileScanTask("t1");
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    GlueTableScan glueTableScan =
        mockGlueTableScanWithCache(
            Expressions.alwaysTrue(), GlueTestUtil.TEST_SCHEMA.select("id"), t1);

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().addShards("shard1").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().addFileScanTasks(t2).build())));
    CloseableIterable<FileScanTask> scanResult = glueTableScan.planFiles();

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(scanResult)).containsExactly(t2);
    Assertions.assertThat(scanResult).doesNotContain(t1);
  }

  @Test
  public void testReuseScanCacheMissByExpression() {
    FileScanTask t1 = GlueTestUtil.createTestFileScanTask("t1");
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    GlueTableScan glueTableScan =
        mockGlueTableScanWithCache(Expressions.equal("id", 10), GlueTestUtil.TEST_SCHEMA, t1);

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/preplan"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PreplanTableResponseParser.toJson(
                        ImmutablePreplanTableResponse.builder().addShards("shard1").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().addFileScanTasks(t2).build())));
    CloseableIterable<FileScanTask> scanResult =
        glueTableScan.filter(Expressions.equal("id", 5)).planFiles();

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(scanResult)).containsExactly(t2);
    Assertions.assertThat(scanResult).doesNotContain(t1);
  }

  private GlueTableScan mockGlueTableScanWithCache(
      Expression expression, Schema cacheKeySchema, FileScanTask... scanTasks) {
    GlueTable glueTable = mockGlueTable();
    Mockito.doReturn(true).when(glueTable).isFileScanTaskCachingEnabled();
    GlueTableScan glueTableScan =
        new GlueTableScan(glueTable, GlueTestUtil.TEST_SCHEMA, TableScanContext.empty());
    ScanCacheKey cacheKey = new ScanCacheKey(expression, cacheKeySchema);
    PlannedCachingScanTaskIterable cachedScanTask =
        GlueTestUtil.createCachedScanTaskIterable(scanTasks);
    glueTable.getScanCache().put(cacheKey, cachedScanTask);
    return glueTableScan;
  }
}
