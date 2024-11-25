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

public class LoadCatalogResponseParser {

  private static final String IDENTIFIER = "identifier";
  private static final String USE_EXTENSIONS = "use-extensions";
  private static final String TARGET_REDSHIFT_CATALOG_IDENTIFIER =
      "target-redshift-catalog-identifier";

  private LoadCatalogResponseParser() {}

  public static String toJson(LoadCatalogResponse response) {
    return toJson(response, false);
  }

  public static String toJson(LoadCatalogResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(LoadCatalogResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid LoadCatalog response: null");
    Preconditions.checkArgument(
        response.identifier() != null, "Invalid LoadCatalog response: identifier must not be null");

    gen.writeStartObject();
    gen.writeStringField(IDENTIFIER, response.identifier());
    gen.writeBooleanField(USE_EXTENSIONS, response.useExtensions());
    gen.writeStringField(
        TARGET_REDSHIFT_CATALOG_IDENTIFIER, response.targetRedshiftCatalogIdentifier());
    gen.writeEndObject();
  }

  public static LoadCatalogResponse fromJson(String json) {
    return JsonUtil.parse(json, LoadCatalogResponseParser::fromJson);
  }

  public static LoadCatalogResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse LoadCatalog response from null object");
    Preconditions.checkArgument(
        json.has(IDENTIFIER), "Cannot parse LoadCatalog response, should have identifier present");
    Preconditions.checkArgument(
        json.has(USE_EXTENSIONS),
        "Cannot parse LoadCatalog response, should have use-extensions present");

    String identifier = JsonUtil.getString(IDENTIFIER, json);
    boolean useExtensions = JsonUtil.getBool(USE_EXTENSIONS, json);
    String targetCatalogArn = JsonUtil.getStringOrNull(TARGET_REDSHIFT_CATALOG_IDENTIFIER, json);

    return ImmutableLoadCatalogResponse.builder()
        .identifier(identifier)
        .useExtensions(useExtensions)
        .targetRedshiftCatalogIdentifier(targetCatalogArn)
        .build();
  }
}
