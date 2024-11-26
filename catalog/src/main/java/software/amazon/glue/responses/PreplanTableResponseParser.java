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

public class PreplanTableResponseParser {

  private static final String SHARDS = "shards";
  private static final String NEXT_PAGE_TOKEN = "next-page-token";

  private PreplanTableResponseParser() {}

  public static String toJson(PreplanTableResponse response) {
    return toJson(response, false);
  }

  public static String toJson(PreplanTableResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(PreplanTableResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid PreplanTable response: null");
    Preconditions.checkArgument(
        response.shards() != null, "Invalid PreplanTable response: shards must not be null");

    gen.writeStartObject();
    JsonUtil.writeStringArray(SHARDS, response.shards(), gen);

    if (response.nextPageToken() != null) {
      gen.writeStringField(NEXT_PAGE_TOKEN, response.nextPageToken());
    }

    gen.writeEndObject();
  }

  public static PreplanTableResponse fromJson(String json) {
    return JsonUtil.parse(json, PreplanTableResponseParser::fromJson);
  }

  public static PreplanTableResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse PreplanTable response from null object");
    Preconditions.checkArgument(
        json.has(SHARDS), "Cannot parse PreplanTable response, should have shards present");

    List<String> shards = JsonUtil.getStringList(SHARDS, json);
    String nextPageToken = JsonUtil.getStringOrNull(NEXT_PAGE_TOKEN, json);

    return ImmutablePreplanTableResponse.builder()
        .shards(shards)
        .nextPageToken(nextPageToken)
        .build();
  }
}
