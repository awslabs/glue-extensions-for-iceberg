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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class TransactionStartedResponseParser {

  private static final String TRANSACTION = "transaction";

  private TransactionStartedResponseParser() {}

  public static String toJson(TransactionStartedResponse response) {
    return toJson(response, false);
  }

  public static String toJson(TransactionStartedResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(TransactionStartedResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid TransactionStartedResponse: null");
    Preconditions.checkArgument(
        response.transaction() != null,
        "Invalid TransactionStartedResponse: transaction must not be null");

    gen.writeStartObject();
    gen.writeStringField(TRANSACTION, response.transaction());
    gen.writeEndObject();
  }

  public static TransactionStartedResponse fromJson(String json) {
    return JsonUtil.parse(json, TransactionStartedResponseParser::fromJson);
  }

  public static TransactionStartedResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse TransactionStartedResponse: null object");
    Preconditions.checkArgument(
        json.has(TRANSACTION), "Cannot parse TransactionStartedResponse: missing transaction");

    String transaction = JsonUtil.getString(TRANSACTION, json);

    return TransactionStartedResponse.builder().transaction(transaction).build();
  }
}
