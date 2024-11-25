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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.glue.GlueResponse;

public class ErrorResponse implements GlueResponse {

  private final String message;
  private final String type;
  private final int code;
  private final List<String> stack;

  private ErrorResponse(String message, String type, int code, List<String> stack) {
    this.message = message;
    this.type = type;
    this.code = code;
    this.stack = stack;
    validate();
  }

  @Override
  public void validate() {
    // Because we use the `ErrorResponseParser`, validation is done there.
  }

  public String message() {
    return message;
  }

  public String type() {
    return type;
  }

  public Integer code() {
    return code;
  }

  public List<String> stack() {
    return stack;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ErrorResponse(")
        .append("code=")
        .append(code)
        .append(", type=")
        .append(type)
        .append(", message=")
        .append(message)
        .append(")");

    if (stack != null && !stack.isEmpty()) {
      for (String line : stack) {
        sb.append("\n").append(line);
      }
    }

    return sb.toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String message;
    private String type;
    private Integer code;
    private List<String> stack;

    private Builder() {}

    public Builder withMessage(String errorMessage) {
      this.message = errorMessage;
      return this;
    }

    public Builder withType(String errorType) {
      this.type = errorType;
      return this;
    }

    public Builder withStackTrace(Throwable throwable) {
      StringWriter sw = new StringWriter();
      try (PrintWriter pw = new PrintWriter(sw)) {
        throwable.printStackTrace(pw);
      }

      this.stack = Arrays.asList(sw.toString().split("\n"));

      return this;
    }

    public Builder withStackTrace(List<String> trace) {
      this.stack = trace;
      return this;
    }

    public Builder responseCode(Integer responseCode) {
      this.code = responseCode;
      return this;
    }

    public ErrorResponse build() {
      Preconditions.checkArgument(code != null, "Invalid response, missing field: code");
      return new ErrorResponse(message, type, code, stack);
    }
  }
}
