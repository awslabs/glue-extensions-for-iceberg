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
package software.amazon.glue.responses;

import java.io.IOException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueExtensionsPaths;
import software.amazon.glue.GlueTestUtil;
import software.amazon.glue.requests.ImmutablePlanTableRequest;

public class TestPlanTableIterable {

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

  @Test
  public void testSingleResponseSingleTaskNoWait() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
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

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1));
  }

  @Test
  public void testSingleResponseMultipleTasksNoWait() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
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
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .build())));

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1, t2));
  }

  @Test
  public void testMultipleResponsesSingleTaskNoWait() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().addFileScanTasks(t2).build())));

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1, t2));
  }

  @Test
  public void testMultipleResponsesMultipleTasksNoWait() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    String t3 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t3"));
    String t4 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t4"));
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t3)
                            .addFileScanTasks(t4)
                            .build())));

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1, t2, t3, t4));
  }

  @Test
  public void testWaitAndGetTasks() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().nextPageToken("page2").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .build())));

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1, t2));
  }

  @Test
  public void testGetTasksAndWait() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().nextPageToken("page3").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page3"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(ImmutablePlanTableResponse.builder().build())));

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1, t2));
  }

  @Test
  public void testGetTasksAndWaitAndGetMoreTasks() {
    String t1 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t1"));
    String t2 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t2"));
    String t3 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t3"));
    String t4 = ScanTaskParser.toJson(GlueTestUtil.createTestFileScanTask("t4"));
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan"),
            Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t1)
                            .addFileScanTasks(t2)
                            .nextPageToken("page2")
                            .build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page2"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder().nextPageToken("page3").build())));

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl/plan")
                .withQueryStringParameter(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, "page3"))
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody(
                    PlanTableResponseParser.toJson(
                        ImmutablePlanTableResponse.builder()
                            .addFileScanTasks(t3)
                            .addFileScanTasks(t4)
                            .build())));

    PlanTableIterable iterable =
        new PlanTableIterable(
            ImmutablePlanTableRequest.builder().shard("shard1").build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/plan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(GlueTestUtil.toSerializedFileScanTasks(iterable))
        .isEqualTo(Lists.newArrayList(t1, t2, t3, t4));
  }
}
