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
package software.amazon.glue.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueTestUtil;
import software.amazon.glue.requests.CreateNamespaceRequest;
import software.amazon.glue.responses.TransactionStartedResponse;

public class TestGlueSigV4Signer {
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
  public void signRequestWithoutBody() {
    HttpRequest request =
        HttpRequest.request()
            .withMethod("DELETE")
            .withPath("/v1/catalogs/:/namespaces/ns/tables/tbl")
            // Require SigV4 Authorization
            .withHeader(Header.header(HttpHeaders.AUTHORIZATION, "AWS4-HMAC-SHA256.*"))
            // Require the empty body checksum
            .withHeader(
                Header.header(
                    SignerConstant.X_AMZ_CONTENT_SHA256, GlueSigV4Signer.EMPTY_BODY_SHA256));

    mockServer
        .when(request)
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody("{\"transaction\": \"tx\"}"));

    TransactionStartedResponse response =
        client.delete(
            "v1/catalogs/:/namespaces/ns/tables/tbl", TransactionStartedResponse.class, e -> {});

    mockServer.verify(request, VerificationTimes.exactly(1));
    assertThat(response).isNotNull();
  }

  @Test
  public void signRequestWithBody() {
    HttpRequest request =
        HttpRequest.request()
            .withMethod("POST")
            .withPath("/v1/catalogs/:/namespaces")
            // Require SigV4 Authorization
            .withHeader(Header.header(HttpHeaders.AUTHORIZATION, "AWS4-HMAC-SHA256.*"))
            // Require a body checksum is set
            .withHeader(Header.header(SignerConstant.X_AMZ_CONTENT_SHA256));

    mockServer
        .when(request)
        .respond(
            HttpResponse.response()
                .withStatusCode(HttpStatus.SC_OK)
                .withBody("{\"transaction\": \"tx\"}"));

    TransactionStartedResponse response =
        client.post(
            "v1/catalogs/:/namespaces",
            CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns")).build(),
            TransactionStartedResponse.class,
            e -> {});

    mockServer.verify(request, VerificationTimes.exactly(1));
    assertThat(response).isNotNull();
  }
}
