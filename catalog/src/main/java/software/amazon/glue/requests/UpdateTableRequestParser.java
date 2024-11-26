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
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class UpdateTableRequestParser {

  private static final String IDENTIFIER = "identifier";
  private static final String REQUIREMENTS = "requirements";
  private static final String UPDATES = "updates";

  private UpdateTableRequestParser() {}

  public static String toJson(UpdateTableRequest request) {
    return toJson(request, false);
  }

  public static String toJson(UpdateTableRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(UpdateTableRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid update table request: null");

    gen.writeStartObject();

    if (null != request.identifier()) {
      gen.writeFieldName(IDENTIFIER);
      TableIdentifierParser.toJson(request.identifier(), gen);
    }

    gen.writeArrayFieldStart(REQUIREMENTS);
    for (UpdateRequirement updateRequirement : request.requirements()) {
      org.apache.iceberg.UpdateRequirementParser.toJson(updateRequirement, gen);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart(UPDATES);
    for (MetadataUpdate metadataUpdate : request.updates()) {
      GlueMetadataUpdateParser.toJson(metadataUpdate, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static UpdateTableRequest fromJson(String json) {
    return JsonUtil.parse(json, UpdateTableRequestParser::fromJson);
  }

  public static UpdateTableRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse update table request from null object");

    TableIdentifier identifier = null;
    List<UpdateRequirement> requirements = Lists.newArrayList();
    List<MetadataUpdate> updates = Lists.newArrayList();

    if (json.hasNonNull(IDENTIFIER)) {
      identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    }

    if (json.hasNonNull(REQUIREMENTS)) {
      JsonNode requirementsNode = JsonUtil.get(REQUIREMENTS, json);
      Preconditions.checkArgument(
          requirementsNode.isArray(),
          "Cannot parse requirements from non-array: %s",
          requirementsNode);
      requirementsNode.forEach(
          req -> requirements.add(org.apache.iceberg.UpdateRequirementParser.fromJson(req)));
    }

    if (json.hasNonNull(UPDATES)) {
      JsonNode updatesNode = JsonUtil.get(UPDATES, json);
      Preconditions.checkArgument(
          updatesNode.isArray(), "Cannot parse metadata updates from non-array: %s", updatesNode);

      updatesNode.forEach(update -> updates.add(GlueMetadataUpdateParser.fromJson(update)));
    }

    return UpdateTableRequest.create(identifier, requirements, updates);
  }
}
