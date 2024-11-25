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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class PlanTableRequestParser {
  private static final String SHARD = "shard";

  private PlanTableRequestParser() {}

  public static String toJson(PlanTableRequest request) {
    return toJson(request, false);
  }

  public static String toJson(PlanTableRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(PlanTableRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid plan table request: null");
    Preconditions.checkArgument(
        null != request.shard(), "Invalid plan table request, requires shard to be non-null");

    gen.writeStartObject();
    gen.writeStringField(SHARD, request.shard());
    gen.writeEndObject();
  }

  public static PlanTableRequest fromJson(String json) {
    return JsonUtil.parse(json, PlanTableRequestParser::fromJson);
  }

  public static PlanTableRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse plan table request from null object");
    Preconditions.checkArgument(json.has(SHARD), "Cannot parse plan table request without shard");
    String shard = JsonUtil.getString(SHARD, json);
    return ImmutablePlanTableRequest.builder().shard(shard).build();
  }
}
