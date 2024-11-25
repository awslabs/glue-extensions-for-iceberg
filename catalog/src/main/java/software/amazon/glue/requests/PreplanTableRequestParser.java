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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class PreplanTableRequestParser {

  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SELECT = "select";
  private static final String FILTER = "filter";
  private static final String METRICS_REQUESTED = "metrics-requested";

  private PreplanTableRequestParser() {}

  public static String toJson(PreplanTableRequest request) {
    return toJson(request, false);
  }

  public static String toJson(PreplanTableRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(PreplanTableRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid pre-plan table request: null");

    gen.writeStartObject();
    if (request.snapshotId() != null) {
      gen.writeNumberField(SNAPSHOT_ID, request.snapshotId());
    }

    if (request.select() != null && !request.select().isEmpty()) {
      JsonUtil.writeStringArray(SELECT, request.select(), gen);
    }

    if (request.filter() != null) {
      gen.writeStringField(FILTER, request.filter());
    }

    if (request.metricsRequested()) {
      gen.writeBooleanField(METRICS_REQUESTED, true);
    }

    gen.writeEndObject();
  }

  public static PreplanTableRequest fromJson(String json) {
    return JsonUtil.parse(json, PreplanTableRequestParser::fromJson);
  }

  public static PreplanTableRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse pre-plan table request from null object");

    Long snapshotId = JsonUtil.getLongOrNull(SNAPSHOT_ID, json);
    List<String> select = JsonUtil.getStringListOrNull(SELECT, json);
    String expression = JsonUtil.getStringOrNull(FILTER, json);
    boolean metricsRequested =
        json.has(METRICS_REQUESTED) && JsonUtil.getBool(METRICS_REQUESTED, json);

    return ImmutablePreplanTableRequest.builder()
        .snapshotId(snapshotId)
        .select(select)
        .filter(expression)
        .metricsRequested(metricsRequested)
        .build();
  }
}
