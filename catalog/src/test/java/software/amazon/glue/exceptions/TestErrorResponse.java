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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestErrorResponse {

  @Test
  public void testErrorResponseToJson() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    String errorModelJson =
        String.format("{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d}", message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";
    ErrorResponse response =
        ErrorResponse.builder().withMessage(message).withType(type).responseCode(code).build();
    assertThat(ErrorResponseParser.toJson(response))
        .as("Should be able to serialize an error response as json")
        .isEqualTo(json);
  }

  @Test
  public void testErrorResponseToJsonWithStack() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    List<String> stack = Arrays.asList("a", "b");
    String errorModelJson =
        String.format(
            "{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d,\"stack\":[\"a\",\"b\"]}",
            message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";
    ErrorResponse response =
        ErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .withStackTrace(stack)
            .build();
    assertThat(ErrorResponseParser.toJson(response))
        .as("Should be able to serialize an error response as json")
        .isEqualTo(json);
  }

  @Test
  public void testErrorResponseFromJson() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    String errorModelJson =
        String.format("{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d}", message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";

    ErrorResponse expected =
        ErrorResponse.builder().withMessage(message).withType(type).responseCode(code).build();
    assertEquals(expected, ErrorResponseParser.fromJson(json));
  }

  @Test
  public void testErrorResponseFromJsonWithStack() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    List<String> stack = Arrays.asList("a", "b");
    String errorModelJson =
        String.format(
            "{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d,\"stack\":[\"a\",\"b\"]}",
            message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";

    ErrorResponse expected =
        ErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .withStackTrace(stack)
            .build();
    assertEquals(expected, ErrorResponseParser.fromJson(json));
  }

  @Test
  public void testErrorResponseFromJsonWithExplicitNullStack() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    List<String> stack = null;
    String errorModelJson =
        String.format(
            "{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d,\"stack\":null}", message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";

    ErrorResponse expected =
        ErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .withStackTrace(stack)
            .build();
    assertEquals(expected, ErrorResponseParser.fromJson(json));
  }

  public void assertEquals(ErrorResponse expected, ErrorResponse actual) {
    assertThat(actual.message()).isEqualTo(expected.message());
    assertThat(actual.type()).isEqualTo(expected.type());
    assertThat(actual.code()).isEqualTo(expected.code());
    assertThat(actual.stack()).isEqualTo(expected.stack());
  }
}
