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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.auth.signer.params.SignerChecksumParams;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.glue.exceptions.GlueExtensionsException;

public class GlueSigV4Signer implements HttpRequestInterceptor {
  static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final String RELOCATED_HEADER_PREFIX = "Original-";

  private static final String SIGNING_NAME = "glue";

  private final Aws4Signer signer = Aws4Signer.create();
  private final AwsCredentialsProvider credentialsProvider;
  private final Region signingRegion;

  public GlueSigV4Signer(Region signingRegion, AwsCredentialsProvider credentialsProvider) {
    this.signingRegion = signingRegion;
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public void process(HttpRequest request, EntityDetails entity, HttpContext context) {
    URI requestUri;

    try {
      requestUri = request.getUri();
    } catch (URISyntaxException e) {
      throw new GlueExtensionsException(e, "Invalid uri for request: %s", request);
    }

    Aws4SignerParams params =
        Aws4SignerParams.builder()
            .signingName(SIGNING_NAME)
            .signingRegion(signingRegion)
            .awsCredentials(credentialsProvider.resolveCredentials())
            .checksumParams(
                SignerChecksumParams.builder()
                    .algorithm(Algorithm.SHA256)
                    .isStreamingRequest(false)
                    .checksumHeaderName(SignerConstant.X_AMZ_CONTENT_SHA256)
                    .build())
            .build();

    SdkHttpFullRequest.Builder sdkRequestBuilder = SdkHttpFullRequest.builder();

    sdkRequestBuilder
        .method(SdkHttpMethod.fromValue(request.getMethod()))
        .protocol(request.getScheme())
        .uri(requestUri)
        .headers(convertHeaders(request.getHeaders()));

    if (entity == null) {
      // This is a workaround for the signer implementation incorrectly producing
      // an invalid content checksum for empty body requests.
      sdkRequestBuilder.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256);
    } else if (entity instanceof StringEntity) {
      sdkRequestBuilder.contentStreamProvider(
          () -> {
            try {
              return ((StringEntity) entity).getContent();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } else {
      throw new UnsupportedOperationException("Unsupported entity type: " + entity.getClass());
    }

    SdkHttpFullRequest signedSdkRequest = signer.sign(sdkRequestBuilder.build(), params);
    updateRequestHeaders(request, signedSdkRequest.headers());
  }

  private Map<String, List<String>> convertHeaders(Header[] headers) {
    return Arrays.stream(headers)
        .collect(
            Collectors.groupingBy(
                // Relocate Authorization header as SigV4 takes precedence
                header ->
                    HttpHeaders.AUTHORIZATION.equals(header.getName())
                        ? RELOCATED_HEADER_PREFIX + header.getName()
                        : header.getName(),
                Collectors.mapping(Header::getValue, Collectors.toList())));
  }

  private void updateRequestHeaders(HttpRequest request, Map<String, List<String>> headers) {
    headers.forEach(
        (name, values) -> {
          if (request.containsHeader(name)) {
            Header[] original = request.getHeaders(name);
            request.removeHeaders(name);
            Arrays.asList(original)
                .forEach(
                    header -> {
                      // Relocate headers if there is a conflict with signed headers
                      if (!values.contains(header.getValue())) {
                        request.addHeader(RELOCATED_HEADER_PREFIX + name, header.getValue());
                      }
                    });
          }

          values.forEach(value -> request.setHeader(name, value));
        });
  }
}
