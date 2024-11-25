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
import java.util.Locale;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import software.amazon.glue.operations.ManifestLocation;
import software.amazon.glue.operations.ManifestType;
import software.amazon.glue.operations.OverwriteRowsWithDeleteFilter;
import software.amazon.glue.operations.OverwriteRowsWithManifest;
import software.amazon.glue.operations.RenameTable;

public class GlueMetadataUpdateParser {
  private static final String ACTION = "action";
  private static final String OVERWRITE_ROWS_WITH_MANIFEST = "overwrite-rows-with-manifest";
  private static final String OVERWRITE_ROWS_WITH_DELETE_FILTER =
      "overwrite-rows-with-delete-filter";
  private static final String RENAME_TABLE = "rename-table";
  private static final String DESTINATION = "destination";
  private static final String ADDED_MANIFEST_LOCATIONS = "added-manifest-locations";
  private static final String REMOVED_MANIFEST_LOCATIONS = "removed-manifest-locations";
  private static final String MANIFEST_LOCATION = "location";
  private static final String MANIFEST_TYPE = "type";
  private static final String DELETE_FILTER = "delete-filter";

  public static String toJson(MetadataUpdate metadataUpdate) {
    return JsonUtil.generate(gen -> toJson(metadataUpdate, gen), false);
  }

  public static void toJson(MetadataUpdate metadataUpdate, JsonGenerator generator)
      throws IOException {
    if (metadataUpdate instanceof OverwriteRowsWithManifest) {
      generator.writeStartObject();
      generator.writeStringField(ACTION, OVERWRITE_ROWS_WITH_MANIFEST);
      writeOverwriteRows((OverwriteRowsWithManifest) metadataUpdate, generator);
      generator.writeEndObject();
    } else if (metadataUpdate instanceof OverwriteRowsWithDeleteFilter) {
      generator.writeStartObject();
      generator.writeStringField(ACTION, OVERWRITE_ROWS_WITH_DELETE_FILTER);
      writeOverwriteRowsWithDeleteFilter((OverwriteRowsWithDeleteFilter) metadataUpdate, generator);
      generator.writeEndObject();
    } else if (metadataUpdate instanceof RenameTable) {
      generator.writeStartObject();
      generator.writeStringField(ACTION, RENAME_TABLE);
      writeRenameTable((RenameTable) metadataUpdate, generator);
      generator.writeEndObject();
    } else {
      MetadataUpdateParser.toJson(metadataUpdate, generator);
    }
  }

  private static void writeOverwriteRows(
      OverwriteRowsWithManifest metadataUpdate, JsonGenerator generator) throws IOException {
    writeManifestLocationArray(
        ADDED_MANIFEST_LOCATIONS, metadataUpdate.getAddedManifestLocations(), generator);
    writeManifestLocationArray(
        REMOVED_MANIFEST_LOCATIONS, metadataUpdate.getRemovedManifestLocations(), generator);
  }

  private static void writeOverwriteRowsWithDeleteFilter(
      OverwriteRowsWithDeleteFilter metadataUpdate, JsonGenerator generator) throws IOException {
    writeManifestLocationArray(
        ADDED_MANIFEST_LOCATIONS, metadataUpdate.getAddedManifestLocations(), generator);
    generator.writeStringField(DELETE_FILTER, metadataUpdate.getDeleteFilter());
  }

  private static void writeManifestLocationArray(
      String property, List<ManifestLocation> manifestLocations, JsonGenerator generator)
      throws IOException {
    generator.writeArrayFieldStart(property);
    if (manifestLocations != null) {
      for (ManifestLocation manifestLocation : manifestLocations) {
        generator.writeStartObject();
        generator.writeStringField(MANIFEST_LOCATION, manifestLocation.getLocation());
        generator.writeStringField(MANIFEST_TYPE, manifestLocation.getType().getManifestType());
        generator.writeEndObject();
      }
    }
    generator.writeEndArray();
  }

  private static void writeRenameTable(RenameTable metadataUpdate, JsonGenerator generator)
      throws IOException {
    generator.writeFieldName(DESTINATION);
    TableIdentifierParser.toJson(metadataUpdate.getDestination(), generator);
  }

  public static MetadataUpdate fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse metadata update from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(
        jsonNode.hasNonNull(ACTION), "Cannot parse metadata update. Missing field: action");
    String action = JsonUtil.getString(ACTION, jsonNode).toLowerCase(Locale.ROOT);
    switch (action) {
      case OVERWRITE_ROWS_WITH_MANIFEST:
        return readOverwriteRowsWithManifest(jsonNode);
      case OVERWRITE_ROWS_WITH_DELETE_FILTER:
        return readOverwriteRowsWithDeleteFilter(jsonNode);
      case RENAME_TABLE:
        return readRenameTable(jsonNode);
      default:
        return MetadataUpdateParser.fromJson(jsonNode);
    }
  }

  private static OverwriteRowsWithManifest readOverwriteRowsWithManifest(JsonNode jsonNode) {
    List<ManifestLocation> getAddedManifestLocations =
        getAddedManifestLocations(ADDED_MANIFEST_LOCATIONS, jsonNode);
    List<ManifestLocation> getRemovedManifestLocations =
        getAddedManifestLocations(REMOVED_MANIFEST_LOCATIONS, jsonNode);
    return new OverwriteRowsWithManifest(getAddedManifestLocations, getRemovedManifestLocations);
  }

  private static OverwriteRowsWithDeleteFilter readOverwriteRowsWithDeleteFilter(
      JsonNode jsonNode) {
    List<ManifestLocation> getAddedManifestLocations =
        getAddedManifestLocations(ADDED_MANIFEST_LOCATIONS, jsonNode);
    String deleteFilter = JsonUtil.getString(DELETE_FILTER, jsonNode);
    return new OverwriteRowsWithDeleteFilter(getAddedManifestLocations, deleteFilter);
  }

  private static List<ManifestLocation> getAddedManifestLocations(
      String property, JsonNode jsonNode) {
    if (!jsonNode.has(property)) {
      return null;
    }
    List<ManifestLocation> manifestLocations = Lists.newArrayList();
    for (JsonNode manifestLocationNode : JsonUtil.get(property, jsonNode)) {
      String location = JsonUtil.getString(MANIFEST_LOCATION, manifestLocationNode);
      String type = JsonUtil.getString(MANIFEST_TYPE, manifestLocationNode);
      manifestLocations.add(new ManifestLocation(location, ManifestType.fromName(type)));
    }
    return manifestLocations;
  }

  private static RenameTable readRenameTable(JsonNode jsonNode) {
    TableIdentifier destination =
        TableIdentifierParser.fromJson(JsonUtil.get(DESTINATION, jsonNode));
    return new RenameTable(destination);
  }
}
