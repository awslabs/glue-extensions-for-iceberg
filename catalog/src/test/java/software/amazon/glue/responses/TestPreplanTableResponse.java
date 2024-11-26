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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestPreplanTableResponse {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PreplanTableResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid PreplanTable response: null");

    assertThatThrownBy(() -> PreplanTableResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse PreplanTable response from null object");
  }

  @Test
  public void roundTripSerdeWithPlanTasks() {
    ImmutablePreplanTableResponse response =
        ImmutablePreplanTableResponse.builder().shards(Lists.newArrayList("a")).build();

    String expectedJson = "{\n" + "  \"shards\" : [ \"a\" ]\n" + "}";

    String json = PreplanTableResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableResponseParser.fromJson(expectedJson)).isEqualTo(response);
  }

  @Test
  public void roundTripSerde() {
    ImmutablePreplanTableResponse response =
        ImmutablePreplanTableResponse.builder()
            .shards(Lists.newArrayList("a", "b"))
            .nextPageToken("token")
            .build();

    String expectedJson =
        "{\n" + "  \"shards\" : [ \"a\", \"b\" ],\n" + "  \"next-page-token\" : \"token\"\n" + "}";

    String json = PreplanTableResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
  }
}
