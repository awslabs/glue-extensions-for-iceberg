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

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.glue.endpoints.GlueEndpointParams;
import software.amazon.awssdk.services.glue.endpoints.GlueEndpointProvider;

public class GlueExtensionsEndpoint implements Serializable {

  public static final String GLUE_ENDPOINT = "glue.endpoint";
  public static final String GLUE_REGION = "client.region";
  public static final String GLUE_EXTENSIONS_ENDPOINT = "glue.extensions.endpoint";
  private static final String GLUE_EXTENSIONS_API_SUFFIX = "/extensions";

  private final String uri;
  private final String region;

  public GlueExtensionsEndpoint(String uri, String region) {
    this.uri = uri;
    this.region = region;
  }

  public static GlueExtensionsEndpoint from(Map<String, String> properties) {
    String glueEndpoint = properties.get(GLUE_ENDPOINT);
    String glueExtensionsEndpoint = properties.get(GLUE_EXTENSIONS_ENDPOINT);
    String glueRegion = properties.get(GLUE_REGION);

    if (glueRegion == null) {
      Region resolvedGlueRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();
      Preconditions.checkArgument(
          resolvedGlueRegion != null,
          "Cannot resolve default AWS region using DefaultAwsRegionProviderChain");
      glueRegion = resolvedGlueRegion.id();
    }

    if (glueExtensionsEndpoint == null) {
      if (glueEndpoint != null) {
        glueExtensionsEndpoint = glueEndpoint + GLUE_EXTENSIONS_API_SUFFIX;
      } else {
        try {
          URI resolvedGlueEndpoint =
              GlueEndpointProvider.defaultProvider()
                  .resolveEndpoint(
                      GlueEndpointParams.builder().region(Region.of(glueRegion)).build())
                  .get()
                  .url();
          glueExtensionsEndpoint = resolvedGlueEndpoint + GLUE_EXTENSIONS_API_SUFFIX;
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException("Fail to retrieve Glue endpoint information", e);
        }
      }
    }

    return new GlueExtensionsEndpoint(glueExtensionsEndpoint, glueRegion);
  }

  public String region() {
    return region;
  }

  public String uri() {
    return uri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GlueExtensionsEndpoint endpoint = (GlueExtensionsEndpoint) o;
    return Objects.equals(uri, endpoint.uri) && Objects.equals(region, endpoint.region);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, region);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GlueExtensionsEndpoint{");
    sb.append("uri='").append(uri).append('\'');
    sb.append(", region='").append(region).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
