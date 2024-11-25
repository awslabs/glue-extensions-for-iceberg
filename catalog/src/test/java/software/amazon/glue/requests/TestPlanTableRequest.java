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
package software.amazon.glue.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

public class TestPlanTableRequest {
  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PlanTableRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid plan table request: null");

    assertThatThrownBy(() -> PlanTableRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse plan table request from null object");
  }

  @Test
  public void roundTripSerdeWithAllFields() {
    ImmutablePlanTableRequest request = ImmutablePlanTableRequest.builder().shard("shard").build();

    String expectedJson = "{\n" + "  \"shard\" : \"shard\"\n" + "}";

    String json = PlanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PlanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }
}
