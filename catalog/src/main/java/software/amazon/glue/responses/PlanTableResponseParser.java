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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class PlanTableResponseParser {
  static final String FILE_SCAN_TASKS = "file-scan-tasks";
  static final String NEXT_PAGE_TOKEN = "next-page-token";

  private PlanTableResponseParser() {}

  public static String toJson(PlanTableResponse response) {
    return toJson(response, false);
  }

  public static String toJson(PlanTableResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(PlanTableResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(
        null != response, "Invalid planTable response in serialization: null");
    Preconditions.checkArgument(
        response.fileScanTasks() != null, "Invalid planTable response, file-scan-tasks: null");

    gen.writeStartObject();
    gen.writeArrayFieldStart(FILE_SCAN_TASKS);
    if (response.fileScanTasks() != null) {
      for (String fileScanTask : response.fileScanTasks()) {
        gen.writeString(fileScanTask);
      }
      gen.writeEndArray();
    }

    if (response.nextPageToken() != null) {
      gen.writeStringField(NEXT_PAGE_TOKEN, response.nextPageToken());
    }
    gen.writeEndObject();
  }

  public static PlanTableResponse fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse plan table response from null");
    return JsonUtil.parse(json, PlanTableResponseParser::fromJson);
  }

  public static PlanTableResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(),
        "Cannot parse plan table response from empty or null object");
    Preconditions.checkArgument(
        json.hasNonNull(FILE_SCAN_TASKS), "Cannot parse file-scan-tasks from null");

    List<String> fileScanTasksJson = JsonUtil.getStringList(FILE_SCAN_TASKS, json);

    String nextPageToken = null;
    if (json.get(NEXT_PAGE_TOKEN) != null) {
      nextPageToken = String.valueOf(JsonUtil.getString(NEXT_PAGE_TOKEN, json));
    }

    return ImmutablePlanTableResponse.builder()
        .fileScanTasks(fileScanTasksJson)
        .nextPageToken(nextPageToken)
        .build();
  }
}
