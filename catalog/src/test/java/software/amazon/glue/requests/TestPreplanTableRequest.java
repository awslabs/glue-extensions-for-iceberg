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
import java.io.IOException;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import software.amazon.glue.GlueTestUtil;

class TestPreplanTableRequest {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PreplanTableRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid pre-plan table request: null");

    assertThatThrownBy(() -> PreplanTableRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse pre-plan table request from null object");
  }

  @Test
  public void roundTripSerdeWithEmptyObject() {
    ImmutablePreplanTableRequest request = ImmutablePreplanTableRequest.builder().build();

    String expectedJson = "{ }";

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }

  @Test
  public void roundTripSerdeWithSnapshotId() {
    ImmutablePreplanTableRequest request =
        ImmutablePreplanTableRequest.builder().snapshotId(1L).build();

    String expectedJson = "{\n  \"snapshot-id\" : 1\n}";

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }

  @Test
  public void roundTripSerdeWithSelectField() {
    ImmutablePreplanTableRequest request =
        ImmutablePreplanTableRequest.builder()
            .snapshotId(1L)
            .select(Lists.newArrayList("col1", "col2"))
            .build();

    String expectedJson =
        "{\n" + "  \"snapshot-id\" : 1,\n" + "  \"select\" : [ \"col1\", \"col2\" ]\n" + "}";

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }

  @Test
  public void roundTripSerdeWithFilterField() {
    ImmutablePreplanTableRequest request =
        ImmutablePreplanTableRequest.builder()
            .snapshotId(1L)
            .filter(ExpressionParser.toJson(Expressions.alwaysFalse()))
            .build();

    String expectedJson = "{\n" + "  \"snapshot-id\" : 1,\n" + "  \"filter\" : \"false\"\n" + "}";

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }

  @Test
  public void roundTripSerdeWithMetricsRequestedField() {
    ImmutablePreplanTableRequest request =
        ImmutablePreplanTableRequest.builder().snapshotId(1L).metricsRequested(true).build();

    String expectedJson =
        "{\n" + "  \"snapshot-id\" : 1,\n" + "  \"metrics-requested\" : true\n" + "}";

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }

  @Test
  public void roundTripSerdeWithAllFields() {
    ImmutablePreplanTableRequest request =
        ImmutablePreplanTableRequest.builder()
            .snapshotId(1L)
            .select(Lists.newArrayList("col1", "col2"))
            .filter(ExpressionParser.toJson(Expressions.alwaysFalse()))
            .metricsRequested(true)
            .build();

    String expectedJson =
        "{\n"
            + "  \"snapshot-id\" : 1,\n"
            + "  \"select\" : [ \"col1\", \"col2\" ],\n"
            + "  \"filter\" : \"false\",\n"
            + "  \"metrics-requested\" : true\n"
            + "}";

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PreplanTableRequestParser.fromJson(expectedJson)).isEqualTo(request);
  }

  @Test
  public void roundTripSerdeWithComplexExpression() throws IOException {
    ImmutablePreplanTableRequest request =
        ImmutablePreplanTableRequest.builder()
            .select(Lists.newArrayList("col1", "col2"))
            .filter(
                ExpressionParser.toJson(
                    Expressions.and(
                        Expressions.equal("col1", "A"), Expressions.greaterThan("col2", 2))))
            .build();

    String expectedJson =
        GlueTestUtil.readResourceFile("PreplanTableRequestComplexExpression.json");

    String json = PreplanTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    // Equality check for Expression type does not work well. So converting to String for equality
    // check.
    assertThat(PreplanTableRequestParser.fromJson(expectedJson).toString())
        .isEqualTo(request.toString());
  }
}
