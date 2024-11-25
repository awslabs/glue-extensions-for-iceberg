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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import software.amazon.glue.exceptions.ErrorResponse;
import software.amazon.glue.exceptions.ErrorResponseParser;

public class TestGlueExtensionsClient {

  private static ClientAndServer mockServer;
  private static GlueExtensionsClient glueExtensionsClient;

  @BeforeAll
  public static void beforeClass() {
    mockServer = startClientAndServer();
    glueExtensionsClient =
        GlueExtensionsClient.builder()
            .withEndpoint(GlueTestUtil.mockServerEndpoint(mockServer))
            .withProperties(GlueTestUtil.TEST_PROPERTIES)
            .withSessionProperties(GlueTestUtil.TEST_SESSION_PROPERTIES)
            .build();
  }

  @AfterAll
  public static void stopServer() throws IOException {
    mockServer.stop();
    glueExtensionsClient.close();
  }

  @Test
  public void testPostSuccess() throws Exception {
    testHttpMethodOnSuccess(HttpMethod.POST);
  }

  @Test
  public void testPostFailure() throws Exception {
    testHttpMethodOnFailure(HttpMethod.POST);
  }

  @Test
  public void testGetSuccess() throws Exception {
    testHttpMethodOnSuccess(HttpMethod.GET);
  }

  @Test
  public void testGetFailure() throws Exception {
    testHttpMethodOnFailure(HttpMethod.GET);
  }

  @Test
  public void testDeleteSuccess() throws Exception {
    testHttpMethodOnSuccess(HttpMethod.DELETE);
  }

  @Test
  public void testDeleteFailure() throws Exception {
    testHttpMethodOnFailure(HttpMethod.DELETE);
  }

  public static void testHttpMethodOnSuccess(HttpMethod method) throws JsonProcessingException {
    Item body = new Item(0L, "hank");
    int statusCode = 200;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(new RuntimeException("Failure response")).when(onError).accept(any());

    String path = addRequestTestCaseAndGetPath(method, body, statusCode);

    Item successResponse = doExecuteRequest(method, path, body, onError);

    if (method.usesRequestBody()) {
      assertThat(body)
          .as("On a successful " + method + ", the correct response body should be returned")
          .isEqualTo(successResponse);
    }

    verify(onError, never()).accept(any());
  }

  public static void testHttpMethodOnFailure(HttpMethod method) throws JsonProcessingException {
    Item body = new Item(0L, "hank");
    int statusCode = 404;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(
            new RuntimeException(
                String.format(
                    "Called error handler for method %s due to status code: %d",
                    method, statusCode)))
        .when(onError)
        .accept(any());

    String path = addRequestTestCaseAndGetPath(method, body, statusCode);

    assertThatThrownBy(() -> doExecuteRequest(method, path, body, onError))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            String.format(
                "Called error handler for method %s due to status code: %d", method, statusCode));

    verify(onError).accept(any());
  }

  // Adds a request that the mock-server can match against, based on the method, path, body, and
  // headers.
  // Return the path generated for the test case, so that the client can call that path to exercise
  // it.
  private static String addRequestTestCaseAndGetPath(HttpMethod method, Item body, int statusCode)
      throws JsonProcessingException {

    // Build the path route, which must be unique per test case.
    boolean isSuccess = statusCode == 200;
    // Using different paths keeps the expectations unique for the test's mock server
    String pathName = isSuccess ? "success" : "failure";
    String path = String.format("%s_%s", method, pathName);

    // Build the expected request
    String asJson = body != null ? GlueObjectMapper.mapper().writeValueAsString(body) : null;
    HttpRequest mockRequest =
        request("/" + path).withMethod(method.name().toUpperCase(Locale.ROOT));
    //            .withHeader(GlueExtensionsClient.CLIENT_VERSION_HEADER, icebergBuildFullVersion)
    //            .withHeader(
    //                GlueExtensionsClient.CLIENT_GIT_COMMIT_SHORT_HEADER,
    // icebergBuildGitCommitShort);

    if (method.usesRequestBody()) {
      mockRequest = mockRequest.withBody(asJson);
    }

    // Build the expected response
    HttpResponse mockResponse = response().withStatusCode(statusCode);

    if (method.usesResponseBody()) {
      if (isSuccess) {
        // Simply return the passed in item in the success case.
        mockResponse = mockResponse.withBody(asJson);
      } else {
        ErrorResponse response =
            ErrorResponse.builder().responseCode(statusCode).withMessage("Not found").build();
        mockResponse = mockResponse.withBody(ErrorResponseParser.toJson(response));
      }
    }

    mockServer.when(mockRequest).respond(mockResponse);

    return path;
  }

  private static Item doExecuteRequest(
      HttpMethod method, String path, Item body, ErrorHandler onError) {
    switch (method) {
      case POST:
        return glueExtensionsClient.post(path, body, Item.class, onError);
      case GET:
        return glueExtensionsClient.get(path, Item.class, onError);
      case DELETE:
        return glueExtensionsClient.delete(path, Item.class, onError);
      default:
        throw new IllegalArgumentException(String.format("Invalid method: %s", method));
    }
  }

  public static class Item implements GlueRequest, GlueResponse {
    private Long id;
    private String data;

    // Required for Jackson deserialization
    @SuppressWarnings("unused")
    public Item() {}

    public Item(Long id, String data) {
      this.id = id;
      this.data = data;
    }

    @Override
    public void validate() {}

    @Override
    public int hashCode() {
      return Objects.hash(id, data);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Item item = (Item) o;
      return Objects.equals(id, item.id) && Objects.equals(data, item.data);
    }
  }

  public enum HttpMethod {
    POST("POST", true, true),
    GET("GET", false, true),
    DELETE("DELETE", false, true);

    HttpMethod(String method, boolean usesRequestBody, boolean usesResponseBody) {
      this.usesResponseBody = usesResponseBody;
      this.usesRequestBody = usesRequestBody;
    }

    // Represents whether we presently use a request / response body with this type or not,
    // not necessarily if a body is allowed in the request or response for this HTTP verb.
    //
    // These are used to build valid test cases with `mock-server`.
    private final boolean usesRequestBody;
    private final boolean usesResponseBody;

    public boolean usesResponseBody() {
      return usesResponseBody;
    }

    public boolean usesRequestBody() {
      return usesRequestBody;
    }
  }
}
