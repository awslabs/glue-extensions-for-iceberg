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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public class TestGlueExtensionsEndpoint {

  @Test
  public void testResolveGlueExtensionsEndpoint() {
    String currentRegion = DefaultAwsRegionProviderChain.builder().build().getRegion().toString();
    Assertions.assertThat(GlueExtensionsEndpoint.from(ImmutableMap.of()))
        .isEqualTo(
            new GlueExtensionsEndpoint(
                String.format("https://glue.%s.amazonaws.com/extensions", currentRegion),
                currentRegion));

    Assertions.assertThat(
            GlueExtensionsEndpoint.from(
                ImmutableMap.of(
                    GlueExtensionsEndpoint.GLUE_EXTENSIONS_ENDPOINT, "http://localhost:8080")))
        .isEqualTo(new GlueExtensionsEndpoint("http://localhost:8080", currentRegion));

    Assertions.assertThat(
            GlueExtensionsEndpoint.from(
                ImmutableMap.of(GlueExtensionsEndpoint.GLUE_ENDPOINT, "http://localhost:8080")))
        .isEqualTo(new GlueExtensionsEndpoint("http://localhost:8080/extensions", currentRegion));

    Assertions.assertThat(
            GlueExtensionsEndpoint.from(
                ImmutableMap.of(
                    GlueExtensionsEndpoint.GLUE_EXTENSIONS_ENDPOINT,
                    "http://localhost:8080/ext",
                    GlueExtensionsEndpoint.GLUE_REGION,
                    "ap-south-2")))
        .isEqualTo(new GlueExtensionsEndpoint("http://localhost:8080/ext", "ap-south-2"));

    Assertions.assertThat(
            GlueExtensionsEndpoint.from(
                ImmutableMap.of(
                    GlueExtensionsEndpoint.GLUE_ENDPOINT,
                    "http://localhost:8080/xyz",
                    GlueExtensionsEndpoint.GLUE_REGION,
                    "ap-south-2")))
        .isEqualTo(
            new GlueExtensionsEndpoint("http://localhost:8080/xyz/extensions", "ap-south-2"));

    Assertions.assertThat(
            GlueExtensionsEndpoint.from(
                ImmutableMap.of(GlueExtensionsEndpoint.GLUE_REGION, "ap-south-2")))
        .isEqualTo(
            new GlueExtensionsEndpoint(
                "https://glue.ap-south-2.amazonaws.com/extensions", "ap-south-2"));

    Assertions.assertThat(
            GlueExtensionsEndpoint.from(
                ImmutableMap.of(GlueExtensionsEndpoint.GLUE_REGION, "us-iso-east-1")))
        .isEqualTo(
            new GlueExtensionsEndpoint(
                "https://glue.us-iso-east-1.c2s.ic.gov/extensions", "us-iso-east-1"));
  }
}
