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
import software.amazon.glue.requests.ImmutablePreplanTableRequest;

public class TestPreplanTableIterable {

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
  public void testSingleResponseSingleShardNoWait() {
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

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable)).isEqualTo(Lists.newArrayList("shard1"));
  }

  @Test
  public void testSingleResponseMultipleShardsNoWait() {
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
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard1")
                            .addShards("shard2")
                            .build())));

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable))
        .isEqualTo(Lists.newArrayList("shard1", "shard2"));
  }

  @Test
  public void testMultipleResponsesSingleShardNoWait() {
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

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable))
        .isEqualTo(Lists.newArrayList("shard1", "shard2"));
  }

  @Test
  public void testMultipleResponsesMultipleShardsNoWait() {
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
                            .addShards("shard2")
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
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard3")
                            .addShards("shard4")
                            .build())));

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable))
        .isEqualTo(Lists.newArrayList("shard1", "shard2", "shard3", "shard4"));
  }

  @Test
  public void testWaitAndGetShards() {
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
                        ImmutablePreplanTableResponse.builder().nextPageToken("page2").build())));

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
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard1")
                            .addShards("shard2")
                            .build())));

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable))
        .isEqualTo(Lists.newArrayList("shard1", "shard2"));
  }

  @Test
  public void testGetShardsAndWait() {
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
                            .addShards("shard2")
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
                        ImmutablePreplanTableResponse.builder().build())));

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable))
        .isEqualTo(Lists.newArrayList("shard1", "shard2"));
  }

  @Test
  public void testGetShardsAndWaitAndGetMoreShards() {
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
                            .addShards("shard2")
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
                        ImmutablePreplanTableResponse.builder()
                            .addShards("shard3")
                            .addShards("shard4")
                            .build())));

    PreplanTableIterable iterable =
        new PreplanTableIterable(
            ImmutablePreplanTableRequest.builder().build(),
            client,
            "v1/catalogs/:/namespaces/ns/tables/tbl/preplan",
            GlueTestUtil.TEST_PROPERTIES);

    Assertions.assertThat(Lists.newArrayList(iterable))
        .isEqualTo(Lists.newArrayList("shard1", "shard2", "shard3", "shard4"));
  }
}
