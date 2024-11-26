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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.impl.EnglishReasonPhraseCatalog;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.glue.auth.GlueSigV4Signer;
import software.amazon.glue.exceptions.ErrorResponse;
import software.amazon.glue.exceptions.GlueExtensionsException;

public class GlueExtensionsClient implements Closeable, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(GlueExtensionsClient.class);

  private final GlueExtensionsEndpoint endpoint;
  private final GlueExtensionsProperties properties;
  private final GlueExtensionsSessionProperties sessionProperties;
  private transient volatile CloseableHttpClient httpClient;

  protected GlueExtensionsClient(
      GlueExtensionsEndpoint endpoint,
      GlueExtensionsProperties properties,
      GlueExtensionsSessionProperties sessionProperties) {
    this.endpoint = endpoint;
    this.properties = properties;
    this.sessionProperties = sessionProperties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public GlueExtensionsEndpoint endpoint() {
    return endpoint;
  }

  private CloseableHttpClient httpClient() {
    if (httpClient == null) {
      synchronized (this) {
        if (httpClient == null) {
          httpClient =
              HttpClients.custom()
                  .setConnectionManager(configureConnectionManager(properties))
                  .setRetryStrategy(
                      new ExponentialHttpRequestRetryStrategy(properties.httpClientMaxRetries()))
                  .addRequestInterceptorLast(
                      new GlueSigV4Signer(
                          Region.of(endpoint.region()), sessionProperties.credentialsProvider()))
                  .build();
        }
      }
    }
    return httpClient;
  }

  private static String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      if (response.getEntity() == null) {
        return null;
      }

      // EntityUtils.toString returns null when HttpEntity.getContent returns null.
      return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
    } catch (IOException | ParseException e) {
      throw new GlueExtensionsException(e, "Failed to convert HTTP response body to string");
    }
  }

  private static boolean isSuccessful(CloseableHttpResponse response) {
    int code = response.getCode();
    return code == HttpStatus.SC_OK
        || code == HttpStatus.SC_ACCEPTED
        || code == HttpStatus.SC_NO_CONTENT;
  }

  private static ErrorResponse buildDefaultErrorResponse(CloseableHttpResponse response) {
    String responseReason = response.getReasonPhrase();
    String message =
        responseReason != null && !responseReason.isEmpty()
            ? responseReason
            : EnglishReasonPhraseCatalog.INSTANCE.getReason(response.getCode(), null /* ignored */);
    return ErrorResponse.builder()
        .responseCode(response.getCode())
        .withMessage(message)
        .withType(GlueExtensionsException.class.getName())
        .build();
  }

  // Process a failed response through the provided errorHandler, and throw a
  // GlueExtensionsException if the
  // provided error handler doesn't already throw.
  private static void throwFailure(
      CloseableHttpResponse response, String responseBody, Consumer<ErrorResponse> errorHandler) {
    ErrorResponse errorResponse = null;

    if (responseBody != null) {
      try {
        if (errorHandler instanceof ErrorHandler) {
          errorResponse =
              ((ErrorHandler) errorHandler).parseResponse(response.getCode(), responseBody);
        } else {
          LOG.warn(
              "Unknown error handler {}, response body won't be parsed",
              errorHandler.getClass().getName());
          errorResponse =
              ErrorResponse.builder()
                  .responseCode(response.getCode())
                  .withMessage(responseBody)
                  .build();
        }

      } catch (UncheckedIOException | IllegalArgumentException e) {
        // It's possible to receive a non-successful response that isn't a properly defined
        // ErrorResponse
        // without any bugs in the server implementation. So we ignore this exception and build an
        // error
        // response for the user.
        //
        // For example, the connection could time out before every reaching the server, in which
        // case we'll
        // likely get a 5xx with the load balancers default 5xx response.
        LOG.error("Failed to parse an error response. Will create one instead.", e);
      }
    }

    if (errorResponse == null) {
      errorResponse = buildDefaultErrorResponse(response);
    }

    errorHandler.accept(errorResponse);

    // Throw an exception in case the provided error handler does not throw.
    throw new GlueExtensionsException("Unhandled error: %s", errorResponse);
  }

  private URI buildUri(String path, Map<String, String> params) {
    // if full path is provided, use the input path as path
    if (path.startsWith("/")) {
      throw new GlueExtensionsException(
          "Received a malformed path for a Glue extensions API request: %s. Paths should not start with /",
          path);
    }
    String fullPath =
        (path.startsWith("https://") || path.startsWith("http://"))
            ? path
            : String.format("%s/%s", endpoint.uri(), path);
    try {
      URIBuilder builder = new URIBuilder(fullPath);
      if (params != null) {
        params.forEach(builder::addParameter);
      }
      return builder.build();
    } catch (URISyntaxException e) {
      throw new GlueExtensionsException(
          "Failed to create request URI from base %s, params %s", fullPath, params);
    }
  }

  private <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler) {
    return execute(method, path, queryParams, requestBody, responseType, errorHandler, h -> {});
  }

  private <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), buildUri(path, queryParams));
    addRequestHeaders(request, ContentType.APPLICATION_JSON.getMimeType());
    if (requestBody != null) {
      request.setEntity(toJson(requestBody));
    }

    try {
      CloseableHttpResponse response = httpClient().execute(request);
      Map<String, String> respHeaders = Maps.newHashMap();
      for (Header header : response.getHeaders()) {
        respHeaders.put(header.getName(), header.getValue());
      }

      responseHeaders.accept(respHeaders);

      // Skip parsing the response stream for any successful request not expecting a response body
      if (response.getCode() == HttpStatus.SC_NO_CONTENT
          || (responseType == null && isSuccessful(response))) {
        return null;
      }

      String responseBody = extractResponseBodyAsString(response);

      if (!isSuccessful(response)) {
        // The provided error handler is expected to throw, but a GlueExtensionsException is thrown
        // if not.
        throwFailure(response, responseBody, errorHandler);
      }

      if (responseBody == null) {
        throw new GlueExtensionsException(
            "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
            responseType.getSimpleName(), method.name(), path, response.getCode());
      }

      try {
        return GlueObjectMapper.mapper().readValue(responseBody, responseType);
      } catch (JsonProcessingException e) {
        throw new GlueExtensionsException(
            e,
            "Received a success response code of %d, but failed to parse response body into %s",
            response.getCode(),
            responseType.getSimpleName());
      }
    } catch (IOException e) {
      throw new GlueExtensionsException(e, "Error occurred while processing %s request", method);
    }
  }

  public <T extends GlueResponse> T get(
      String path, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    return get(path, ImmutableMap.of(), responseType, errorHandler);
  }

  public <T extends GlueResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.GET, path, queryParams, null, responseType, errorHandler);
  }

  public <T extends GlueResponse> T post(
      String path, GlueRequest body, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    return post(path, ImmutableMap.of(), body, responseType, errorHandler);
  }

  public <T extends GlueResponse> T post(
      String path,
      Map<String, String> queryParms,
      GlueRequest body,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.POST, path, queryParms, body, responseType, errorHandler);
  }

  public <T extends GlueResponse> T delete(
      String path, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    return delete(path, ImmutableMap.of(), responseType, errorHandler);
  }

  public <T extends GlueResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.DELETE, path, queryParams, null, responseType, errorHandler);
  }

  private void addRequestHeaders(HttpUriRequest request, String bodyMimeType) {
    request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    // Many systems require that content type is set regardless and will fail, even on an empty
    // bodied request.
    request.setHeader(HttpHeaders.CONTENT_TYPE, bodyMimeType);
  }

  @Override
  public void close() throws IOException {
    httpClient().close(CloseMode.GRACEFUL);
  }

  static HttpClientConnectionManager configureConnectionManager(
      GlueExtensionsProperties properties) {
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create();

    ConnectionConfig connectionConfig =
        ConnectionConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(properties.httpClientConnectionTimeoutMs()))
            .setSocketTimeout(Timeout.ofMilliseconds(properties.httpClientSocketTimeoutMs()))
            .build();
    connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);

    return connectionManagerBuilder
        .useSystemProperties()
        .setMaxConnTotal(properties.httpClientMaxConnections())
        .setMaxConnPerRoute(properties.httpClientMaxConnectionsPerRoute())
        .build();
  }

  public static class Builder {

    private GlueExtensionsEndpoint endpoint;
    private GlueExtensionsPaths paths;
    private GlueExtensionsProperties properties;
    private GlueExtensionsSessionProperties sessionProperties;

    public Builder() {}

    public Builder withEndpoint(GlueExtensionsEndpoint endpointInput) {
      Preconditions.checkNotNull(endpointInput, "endpoint must not be null");
      this.endpoint = endpointInput;
      return this;
    }

    public Builder withSessionProperties(GlueExtensionsSessionProperties sessionPropertiesInput) {
      Preconditions.checkNotNull(sessionPropertiesInput, "sessionProperties must not be null");
      this.sessionProperties = sessionPropertiesInput;
      return this;
    }

    public Builder withProperties(GlueExtensionsProperties propertiesInput) {
      Preconditions.checkNotNull(propertiesInput, "properties must not be null");
      this.properties = propertiesInput;
      return this;
    }

    public GlueExtensionsClient build() {
      return new GlueExtensionsClient(endpoint, properties, sessionProperties);
    }
  }

  private StringEntity toJson(Object requestBody) {
    try {
      return new StringEntity(
          GlueObjectMapper.mapper().writeValueAsString(requestBody), StandardCharsets.UTF_8);
    } catch (JsonProcessingException e) {
      throw new GlueExtensionsException(e, "Failed to write request body: %s", requestBody);
    }
  }
}
