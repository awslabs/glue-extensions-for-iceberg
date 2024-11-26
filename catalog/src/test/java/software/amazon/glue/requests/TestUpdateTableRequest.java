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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import software.amazon.glue.operations.ManifestLocation;
import software.amazon.glue.operations.ManifestType;
import software.amazon.glue.operations.OverwriteRowsWithDeleteFilter;
import software.amazon.glue.operations.OverwriteRowsWithManifest;
import software.amazon.glue.operations.RenameTable;

public class TestUpdateTableRequest {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> UpdateTableRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid update table request: null");

    assertThatThrownBy(() -> UpdateTableRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update table request from null object");

    UpdateTableRequest request = UpdateTableRequestParser.fromJson("{}");
    assertThat(request.identifier()).isNull();
    assertThat(request.updates()).isEmpty();
    assertThat(request.requirements()).isEmpty();
  }

  @Test
  public void invalidTableIdentifier() {
    // table identifier is optional
    UpdateTableRequest request =
        UpdateTableRequestParser.fromJson("{\"requirements\" : [], \"updates\" : []}");
    assertThat(request.identifier()).isNull();

    assertThatThrownBy(() -> UpdateTableRequestParser.fromJson("{\"identifier\" : {}}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    assertThatThrownBy(
            () -> UpdateTableRequestParser.fromJson("{\"identifier\" : { \"name\": 23}}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: name: 23");
  }

  @Test
  public void invalidRequirements() {
    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[23],\"updates\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update requirement from non-object value: 23");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[{}],\"updates\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update requirement. Missing field: type");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[{\"type\":\"assert-table-uuid\"}],\"updates\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uuid");
  }

  @Test
  public void invalidMetadataUpdates() {
    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[],\"updates\":[23]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata update from non-object value: 23");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[],\"updates\":[{}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata update. Missing field: action");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[],\"updates\":[{\"action\":\"assign-uuid\"}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uuid");
  }

  @Test
  public void roundTripSerde() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TableIdentifier.of("ns1", "table1"),
            ImmutableList.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid),
                new org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    String expectedJson =
        "{\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"ns1\" ],\n"
            + "    \"name\" : \"table1\"\n"
            + "  },\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"type\" : \"assert-create\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"action\" : \"set-current-schema\",\n"
            + "    \"schema-id\" : 23\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithoutIdentifier() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid),
                new org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"type\" : \"assert-create\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"action\" : \"set-current-schema\",\n"
            + "    \"schema-id\" : 23\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void emptyRequirementsAndUpdates() {
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TableIdentifier.of("ns1", "table1"), ImmutableList.of(), ImmutableList.of());

    String json =
        "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},\"requirements\":[],\"updates\":[]}";

    assertThat(UpdateTableRequestParser.toJson(request)).isEqualTo(json);
    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json)))
        .isEqualTo(json);

    assertThat(UpdateTableRequestParser.toJson(request)).isEqualTo(json);
    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(
            UpdateTableRequestParser.toJson(
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"}}")))
        .isEqualTo(json);
  }

  @Test
  public void roundTripWithOverwriteFilesWithManifests() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String manifests = "s3://bucket/path/to/file.json";
    ManifestLocation manifestLocation = new ManifestLocation(manifests, ManifestType.REDSHIFT);
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid)),
            ImmutableList.of(
                new OverwriteRowsWithManifest(
                    ImmutableList.of(manifestLocation), ImmutableList.of(manifestLocation))));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"overwrite-rows-with-manifest\",\n"
            + "    \"added-manifest-locations\" : [ {\n"
            + "      \"location\" : \"s3://bucket/path/to/file.json\",\n"
            + "      \"type\" : \"redshift\"\n"
            + "    } ],\n"
            + "    \"removed-manifest-locations\" : [ {\n"
            + "      \"location\" : \"s3://bucket/path/to/file.json\",\n"
            + "      \"type\" : \"redshift\"\n"
            + "    } ]\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripWithAppendFilesUpdate() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String manifests = "s3://bucket/path/to/file.json";
    ManifestLocation manifestLocation = new ManifestLocation(manifests, ManifestType.REDSHIFT);
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid)),
            ImmutableList.of(
                new OverwriteRowsWithManifest(ImmutableList.of(manifestLocation), null)));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"overwrite-rows-with-manifest\",\n"
            + "    \"added-manifest-locations\" : [ {\n"
            + "      \"location\" : \"s3://bucket/path/to/file.json\",\n"
            + "      \"type\" : \"redshift\"\n"
            + "    } ],\n"
            + "    \"removed-manifest-locations\" : [ ]\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripWithDeleteFilesUpdate() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String manifests = "s3://bucket/path/to/file.json";
    ManifestLocation manifestLocation = new ManifestLocation(manifests, ManifestType.REDSHIFT);
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid)),
            ImmutableList.of(
                new OverwriteRowsWithManifest(null, ImmutableList.of(manifestLocation))));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"overwrite-rows-with-manifest\",\n"
            + "    \"added-manifest-locations\" : [ ],\n"
            + "    \"removed-manifest-locations\" : [ {\n"
            + "      \"location\" : \"s3://bucket/path/to/file.json\",\n"
            + "      \"type\" : \"redshift\"\n"
            + "    } ]\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripWithRenameTable() {
    TableIdentifier identifier = TableIdentifier.of("ns1", "table1");

    UpdateTableRequest request =
        new UpdateTableRequest(null, ImmutableList.of(new RenameTable(identifier)));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"rename-table\",\n"
            + "    \"destination\" : {\n"
            + "      \"namespace\" : [ \"ns1\" ],\n"
            + "      \"name\" : \"table1\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripWithOverwriteFilesWithDeleteFilter() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String manifests = "s3://bucket/path/to/file.json";
    ManifestLocation manifestLocation = new ManifestLocation(manifests, ManifestType.REDSHIFT);
    Expression expression = Expressions.in("column-name", new BigDecimal("3.14"));
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid)),
            ImmutableList.of(
                new OverwriteRowsWithDeleteFilter(
                    ImmutableList.of(manifestLocation), ExpressionParser.toJson(expression))));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"overwrite-rows-with-delete-filter\",\n"
            + "    \"added-manifest-locations\" : [ {\n"
            + "      \"location\" : \"s3://bucket/path/to/file.json\",\n"
            + "      \"type\" : \"redshift\"\n"
            + "    } ],\n"
            + "    \"delete-filter\" : \"{\\\"type\\\":\\\"in\\\",\\\"term\\\":\\\"column-name\\\",\\\"values\\\":[\\\"3.14\\\"]}\"\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripWithOverwriteFilesWithDeleteFilterNoManifests() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    Expression expression = Expressions.in("column-name", new BigDecimal("3.14"));
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid)),
            ImmutableList.of(
                new OverwriteRowsWithDeleteFilter(null, ExpressionParser.toJson(expression))));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"overwrite-rows-with-delete-filter\",\n"
            + "    \"added-manifest-locations\" : [ ],\n"
            + "    \"delete-filter\" : \"{\\\"type\\\":\\\"in\\\",\\\"term\\\":\\\"column-name\\\",\\\"values\\\":[\\\"3.14\\\"]}\"\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
