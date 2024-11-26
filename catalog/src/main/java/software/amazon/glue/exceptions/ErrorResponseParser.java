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
package software.amazon.glue.exceptions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ErrorResponseParser {

  private ErrorResponseParser() {}

  private static final String ERROR = "error";
  private static final String MESSAGE = "message";
  private static final String TYPE = "type";
  private static final String CODE = "code";
  private static final String STACK = "stack";

  public static String toJson(ErrorResponse errorResponse) {
    return toJson(errorResponse, false);
  }

  public static String toJson(ErrorResponse errorResponse, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(errorResponse, gen), pretty);
  }

  public static void toJson(ErrorResponse errorResponse, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();

    generator.writeObjectFieldStart(ERROR);

    generator.writeStringField(MESSAGE, errorResponse.message());
    generator.writeStringField(TYPE, errorResponse.type());
    generator.writeNumberField(CODE, errorResponse.code());
    if (errorResponse.stack() != null) {
      JsonUtil.writeStringArray(STACK, errorResponse.stack(), generator);
    }

    generator.writeEndObject();

    generator.writeEndObject();
  }

  /**
   * Read ErrorResponse from a JSON string.
   *
   * @param json a JSON string of an ErrorResponse
   * @return an ErrorResponse object
   */
  public static ErrorResponse fromJson(String json) {
    return JsonUtil.parse(json, ErrorResponseParser::fromJson);
  }

  public static ErrorResponse fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse error response from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(jsonNode.has(ERROR), "Cannot parse missing field: error");
    JsonNode error = JsonUtil.get(ERROR, jsonNode);
    String message = JsonUtil.getStringOrNull(MESSAGE, error);
    String type = JsonUtil.getStringOrNull(TYPE, error);
    Integer code = JsonUtil.getIntOrNull(CODE, error);
    List<String> stack = JsonUtil.getStringListOrNull(STACK, error);
    return ErrorResponse.builder()
        .withMessage(message)
        .withType(type)
        .responseCode(code)
        .withStackTrace(stack)
        .build();
  }
}
