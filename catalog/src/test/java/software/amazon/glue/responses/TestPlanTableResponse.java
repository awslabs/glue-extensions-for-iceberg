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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.types.Comparators;
import org.junit.jupiter.api.Test;
import software.amazon.glue.GlueTestUtil;

public class TestPlanTableResponse {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PlanTableResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid planTable response in serialization: null");

    assertThatThrownBy(() -> PlanTableResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse plan table response from empty or null object");
  }

  @Test
  public void emptyPlanTableResponseInvalidCheck() {
    assertThatThrownBy(() -> PlanTableResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse plan table response from empty or null object");
  }

  @Test
  public void nullFileScanTaskListInvalidCheck() {
    assertThatThrownBy(() -> PlanTableResponseParser.fromJson("{\"file-scan-tasks\": null}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse file-scan-tasks from null");
  }

  @Test
  public void emptyFileScanTaskListValidCheck() {
    PlanTableResponse expectedPlanTableResponse =
        ImmutablePlanTableResponse.builder().fileScanTasks(Lists.newArrayList()).build();
    PlanTableResponse response = PlanTableResponseParser.fromJson("{\"file-scan-tasks\": []}");
    assertThat(response).isEqualTo(expectedPlanTableResponse);
  }

  @Test
  public void emptyFileScanTaskListWithPageToken() {
    PlanTableResponse expectedPlanTableResponse =
        ImmutablePlanTableResponse.builder()
            .fileScanTasks(Lists.newArrayList())
            .nextPageToken("token")
            .build();
    PlanTableResponse response =
        PlanTableResponseParser.fromJson(
            "{\"file-scan-tasks\": [], \"next-page-token\": \"token\"}");
    assertThat(response).isEqualTo(expectedPlanTableResponse);
  }

  @Test
  public void nullFileScanTaskCheck() {
    // Invalid file scan task as it is null
    assertThatThrownBy(() -> PlanTableResponseParser.fromJson("{\"file-scan-tasks\": [null]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value in file-scan-tasks: null");
  }

  @Test
  public void nullDataFileCheck() {
    // Invalid data file as it is null
    assertThatThrownBy(
            () ->
                toFileScanTasksList(
                    PlanTableResponseParser.fromJson(
                        GlueTestUtil.readResourceFile("PlanTableResponseNullDataFile.json"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON node for content file: non-object (null)");
  }

  @Test
  public void missingFieldWithinFileScanTaskCheck() {
    // Invalid file scan task as does not contain required schema field
    assertThatThrownBy(
            () ->
                toFileScanTasksList(
                    PlanTableResponseParser.fromJson(
                        GlueTestUtil.readResourceFile(
                            "PlanTableResponseInvalidFileScanTask.json"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: schema");
  }

  @Test
  public void missingFieldWithinDataFileCheck() {
    // Invalid date file as does not contain required file-path field
    assertThatThrownBy(
            () ->
                toFileScanTasksList(
                    PlanTableResponseParser.fromJson(
                        GlueTestUtil.readResourceFile("PlanTableResponseInvalidDataFile.json"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: file-path");
  }

  @Test
  public void roundTripSerdeWithFileScanTasks() {
    List<FileScanTask> fileScanTasks = new ArrayList<>();
    FileScanTask task = GlueTestUtil.createTestFileScanTask();
    fileScanTasks.add(task);
    ImmutablePlanTableResponse planTableResponse =
        ImmutablePlanTableResponse.builder()
            .fileScanTasks(
                fileScanTasks.stream().map(ScanTaskParser::toJson).collect(Collectors.toList()))
            .build();
    String expectedJson = GlueTestUtil.readResourceFile("PlanTableResponseWithFileScanTasks.json");
    String toJson = PlanTableResponseParser.toJson(planTableResponse, true);
    assertThat(toJson).isEqualTo(expectedJson);
    PlanTableResponse fromJsonResponse = PlanTableResponseParser.fromJson(expectedJson);
    assertPlanTableResponseEquals(planTableResponse, fromJsonResponse, GlueTestUtil.TEST_SPEC);
  }

  @Test
  public void roundTripSerdeWithFileScanTasksAndNextPageToken() {
    List<FileScanTask> fileScanTasks = new ArrayList<>();
    FileScanTask task = GlueTestUtil.createTestFileScanTask();
    fileScanTasks.add(task);
    ImmutablePlanTableResponse planTableResponse =
        ImmutablePlanTableResponse.builder()
            .fileScanTasks(
                fileScanTasks.stream().map(ScanTaskParser::toJson).collect(Collectors.toList()))
            .nextPageToken("token")
            .build();
    String expectedJson =
        GlueTestUtil.readResourceFile("PlanTableResponseWithFileScanTasksAndNextPageToken.json");
    String toJson = PlanTableResponseParser.toJson(planTableResponse, true);
    assertThat(toJson).isEqualTo(expectedJson);

    PlanTableResponse fromJsonResponse = PlanTableResponseParser.fromJson(expectedJson);
    assertPlanTableResponseEquals(planTableResponse, fromJsonResponse, GlueTestUtil.TEST_SPEC);
  }

  private static void assertPlanTableResponseEquals(
      ImmutablePlanTableResponse expected, PlanTableResponse actual, PartitionSpec spec) {
    FileScanTask expectedFileScanTask = toFileScanTasksList(expected).get(0);
    FileScanTask actualFileScanTask = toFileScanTasksList(actual).get(0);
    ScanTaskParser.fromJson(actual.fileScanTasks().get(0), false).asFileScanTask();
    assertFileScanTaskEquals(expectedFileScanTask, actualFileScanTask, spec);
    assertThat(expected.nextPageToken()).isEqualTo(actual.nextPageToken());
  }

  private static void assertFileScanTaskEquals(
      FileScanTask expected, FileScanTask actual, PartitionSpec spec) {
    assertContentFileEquals(expected.file(), actual.file(), spec);
    assertThat(actual.deletes()).hasSameSizeAs(expected.deletes());
    for (int pos = 0; pos < expected.deletes().size(); ++pos) {
      assertContentFileEquals(expected.deletes().get(pos), actual.deletes().get(pos), spec);
    }
    assertThat(actual.schema().asStruct()).isEqualTo(expected.schema().asStruct());
    assertThat(actual.spec()).isEqualTo(expected.spec());
    assertThat(
            ExpressionUtil.equivalent(
                expected.residual(),
                actual.residual(),
                org.apache.iceberg.TestBase.SCHEMA.asStruct(),
                false))
        .as("Residual expression should match")
        .isTrue();
  }

  private static void assertContentFileEquals(
      ContentFile<?> expected, ContentFile<?> actual, PartitionSpec spec) {
    assertThat(actual.getClass()).isEqualTo(expected.getClass());
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.path()).isEqualTo(expected.path());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.partition())
        .usingComparator(Comparators.forType(spec.partitionType()))
        .isEqualTo(expected.partition());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.columnSizes()).isEqualTo(expected.columnSizes());
    assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    assertThat(actual.upperBounds()).isEqualTo(expected.upperBounds());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
    assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
  }

  private static List<FileScanTask> toFileScanTasksList(PlanTableResponse response) {
    return response.fileScanTasks().stream()
        .map(s -> ScanTaskParser.fromJson(s, false))
        .collect(Collectors.toList());
  }
}
