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
package software.amazon.glue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.UnboundSortOrder;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.util.JsonUtil;
import software.amazon.glue.exceptions.ErrorResponse;
import software.amazon.glue.exceptions.ErrorResponseParser;
import software.amazon.glue.requests.ImmutablePlanTableRequest;
import software.amazon.glue.requests.ImmutablePreplanTableRequest;
import software.amazon.glue.requests.PlanTableRequest;
import software.amazon.glue.requests.PlanTableRequestParser;
import software.amazon.glue.requests.PreplanTableRequest;
import software.amazon.glue.requests.PreplanTableRequestParser;
import software.amazon.glue.requests.UpdateTableRequest;
import software.amazon.glue.requests.UpdateTableRequestParser;
import software.amazon.glue.responses.ImmutableLoadCatalogResponse;
import software.amazon.glue.responses.ImmutablePlanTableResponse;
import software.amazon.glue.responses.ImmutablePreplanTableResponse;
import software.amazon.glue.responses.LoadCatalogResponse;
import software.amazon.glue.responses.LoadCatalogResponseParser;
import software.amazon.glue.responses.PlanTableResponse;
import software.amazon.glue.responses.PlanTableResponseParser;
import software.amazon.glue.responses.PreplanTableResponse;
import software.amazon.glue.responses.PreplanTableResponseParser;
import software.amazon.glue.responses.TransactionStartedResponse;
import software.amazon.glue.responses.TransactionStartedResponseParser;

public class GlueSerializers {

  private GlueSerializers() {}

  public static void registerAll(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module
        .addSerializer(ErrorResponse.class, new ErrorResponseSerializer())
        .addDeserializer(ErrorResponse.class, new ErrorResponseDeserializer())
        .addSerializer(TableIdentifier.class, new TableIdentifierSerializer())
        .addDeserializer(TableIdentifier.class, new TableIdentifierDeserializer())
        .addSerializer(Namespace.class, new NamespaceSerializer())
        .addDeserializer(Namespace.class, new NamespaceDeserializer())
        .addSerializer(Schema.class, new SchemaSerializer())
        .addDeserializer(Schema.class, new SchemaDeserializer())
        .addSerializer(UnboundPartitionSpec.class, new UnboundPartitionSpecSerializer())
        .addDeserializer(UnboundPartitionSpec.class, new UnboundPartitionSpecDeserializer())
        .addSerializer(UnboundSortOrder.class, new UnboundSortOrderSerializer())
        .addDeserializer(UnboundSortOrder.class, new UnboundSortOrderDeserializer())
        .addSerializer(MetadataUpdate.class, new MetadataUpdateSerializer())
        .addDeserializer(MetadataUpdate.class, new MetadataUpdateDeserializer())
        .addSerializer(TableMetadata.class, new TableMetadataSerializer())
        .addDeserializer(TableMetadata.class, new TableMetadataDeserializer())
        .addSerializer(UpdateRequirement.class, new UpdateReqSerializer())
        .addDeserializer(UpdateRequirement.class, new UpdateReqDeserializer())
        .addSerializer(UpdateTableRequest.class, new UpdateTableRequestSerializer())
        .addDeserializer(UpdateTableRequest.class, new UpdateTableRequestDeserializer())
        .addSerializer(PreplanTableRequest.class, new PreplanTableRequestSerializer<>())
        .addDeserializer(PreplanTableRequest.class, new PreplanTableRequestDeserializer<>())
        .addSerializer(ImmutablePreplanTableRequest.class, new PreplanTableRequestSerializer<>())
        .addDeserializer(
            ImmutablePreplanTableRequest.class, new PreplanTableRequestDeserializer<>())
        .addSerializer(PreplanTableResponse.class, new PreplanTableResponseSerializer<>())
        .addDeserializer(PreplanTableResponse.class, new PreplanTableResponseDeserializer<>())
        .addSerializer(ImmutablePreplanTableResponse.class, new PreplanTableResponseSerializer<>())
        .addDeserializer(
            ImmutablePreplanTableResponse.class, new PreplanTableResponseDeserializer<>())
        .addSerializer(PlanTableRequest.class, new PlanTableRequestSerializer<>())
        .addDeserializer(PlanTableRequest.class, new PlanTableRequestDeserializer<>())
        .addSerializer(ImmutablePlanTableRequest.class, new PlanTableRequestSerializer<>())
        .addDeserializer(ImmutablePlanTableRequest.class, new PlanTableRequestDeserializer<>())
        .addSerializer(PlanTableResponse.class, new PlanTableResponseSerializer<>())
        .addDeserializer(PlanTableResponse.class, new PlanTableResponseDeserializer<>())
        .addSerializer(ImmutablePlanTableResponse.class, new PlanTableResponseSerializer<>())
        .addDeserializer(ImmutablePlanTableResponse.class, new PlanTableResponseDeserializer<>())
        .addSerializer(LoadCatalogResponse.class, new LoadCatalogResponseSerializer<>())
        .addDeserializer(LoadCatalogResponse.class, new LoadCatalogResponseDeserializer<>())
        .addSerializer(ImmutableLoadCatalogResponse.class, new LoadCatalogResponseSerializer<>())
        .addDeserializer(
            ImmutableLoadCatalogResponse.class, new LoadCatalogResponseDeserializer<>())
        .addSerializer(
            TransactionStartedResponse.class, new TransactionStartedResponseSerializer<>())
        .addDeserializer(
            TransactionStartedResponse.class, new TransactionStartedResponseDeserializer<>());

    mapper.registerModule(module);
  }

  static class UpdateReqDeserializer extends JsonDeserializer<UpdateRequirement> {
    @Override
    public UpdateRequirement deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return org.apache.iceberg.UpdateRequirementParser.fromJson(node);
    }
  }

  static class UpdateReqSerializer extends JsonSerializer<UpdateRequirement> {
    @Override
    public void serialize(
        UpdateRequirement value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      org.apache.iceberg.UpdateRequirementParser.toJson(value, gen);
    }
  }

  public static class TableMetadataDeserializer extends JsonDeserializer<TableMetadata> {
    @Override
    public TableMetadata deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return TableMetadataParser.fromJson(node);
    }
  }

  public static class TableMetadataSerializer extends JsonSerializer<TableMetadata> {
    @Override
    public void serialize(TableMetadata metadata, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      TableMetadataParser.toJson(metadata, gen);
    }
  }

  public static class MetadataUpdateDeserializer extends JsonDeserializer<MetadataUpdate> {
    @Override
    public MetadataUpdate deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return MetadataUpdateParser.fromJson(node);
    }
  }

  public static class MetadataUpdateSerializer extends JsonSerializer<MetadataUpdate> {
    @Override
    public void serialize(MetadataUpdate value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      MetadataUpdateParser.toJson(value, gen);
    }
  }

  public static class ErrorResponseDeserializer extends JsonDeserializer<ErrorResponse> {
    @Override
    public ErrorResponse deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return ErrorResponseParser.fromJson(node);
    }
  }

  public static class ErrorResponseSerializer extends JsonSerializer<ErrorResponse> {
    @Override
    public void serialize(
        ErrorResponse errorResponse, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      ErrorResponseParser.toJson(errorResponse, gen);
    }
  }

  public static class NamespaceDeserializer extends JsonDeserializer<Namespace> {
    @Override
    public Namespace deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String[] levels = JsonUtil.getStringArray(p.getCodec().readTree(p));
      return Namespace.of(levels);
    }
  }

  public static class NamespaceSerializer extends JsonSerializer<Namespace> {
    @Override
    public void serialize(Namespace namespace, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      String[] parts = namespace.levels();
      gen.writeArray(parts, 0, parts.length);
    }
  }

  public static class TableIdentifierDeserializer extends JsonDeserializer<TableIdentifier> {
    @Override
    public TableIdentifier deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return TableIdentifierParser.fromJson(jsonNode);
    }
  }

  public static class TableIdentifierSerializer extends JsonSerializer<TableIdentifier> {
    @Override
    public void serialize(
        TableIdentifier identifier, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      TableIdentifierParser.toJson(identifier, gen);
    }
  }

  public static class SchemaDeserializer extends JsonDeserializer<Schema> {
    @Override
    public Schema deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return SchemaParser.fromJson(jsonNode);
    }
  }

  public static class SchemaSerializer extends JsonSerializer<Schema> {
    @Override
    public void serialize(Schema schema, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      SchemaParser.toJson(schema, gen);
    }
  }

  public static class UnboundPartitionSpecSerializer extends JsonSerializer<UnboundPartitionSpec> {
    @Override
    public void serialize(
        UnboundPartitionSpec spec, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      PartitionSpecParser.toJson(spec, gen);
    }
  }

  public static class UnboundPartitionSpecDeserializer
      extends JsonDeserializer<UnboundPartitionSpec> {
    @Override
    public UnboundPartitionSpec deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return PartitionSpecParser.fromJson(jsonNode);
    }
  }

  public static class UnboundSortOrderSerializer extends JsonSerializer<UnboundSortOrder> {
    @Override
    public void serialize(
        UnboundSortOrder sortOrder, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      SortOrderParser.toJson(sortOrder, gen);
    }
  }

  public static class UnboundSortOrderDeserializer extends JsonDeserializer<UnboundSortOrder> {
    @Override
    public UnboundSortOrder deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return SortOrderParser.fromJson(jsonNode);
    }
  }

  public static class UpdateTableRequestSerializer extends JsonSerializer<UpdateTableRequest> {
    @Override
    public void serialize(
        UpdateTableRequest request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      UpdateTableRequestParser.toJson(request, gen);
    }
  }

  public static class UpdateTableRequestDeserializer extends JsonDeserializer<UpdateTableRequest> {
    @Override
    public UpdateTableRequest deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return UpdateTableRequestParser.fromJson(jsonNode);
    }
  }

  static class PreplanTableRequestSerializer<T extends PreplanTableRequest>
      extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      PreplanTableRequestParser.toJson(request, gen);
    }
  }

  static class PreplanTableRequestDeserializer<T extends PreplanTableRequest>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) PreplanTableRequestParser.fromJson(jsonNode);
    }
  }

  static class PreplanTableResponseSerializer<T extends PreplanTableResponse>
      extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      PreplanTableResponseParser.toJson(request, gen);
    }
  }

  static class PreplanTableResponseDeserializer<T extends PreplanTableResponse>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) PreplanTableResponseParser.fromJson(jsonNode);
    }
  }

  static class PlanTableRequestSerializer<T extends PlanTableRequest> extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      PlanTableRequestParser.toJson(request, gen);
    }
  }

  static class PlanTableRequestDeserializer<T extends PlanTableRequest>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) PlanTableRequestParser.fromJson(jsonNode);
    }
  }

  static class PlanTableResponseSerializer<T extends PlanTableResponse> extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      PlanTableResponseParser.toJson(request, gen);
    }
  }

  static class PlanTableResponseDeserializer<T extends PlanTableResponse>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) PlanTableResponseParser.fromJson(jsonNode);
    }
  }

  static class LoadCatalogResponseSerializer<T extends LoadCatalogResponse>
      extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      LoadCatalogResponseParser.toJson(request, gen);
    }
  }

  static class LoadCatalogResponseDeserializer<T extends LoadCatalogResponse>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) LoadCatalogResponseParser.fromJson(jsonNode);
    }
  }

  static class TransactionStartedResponseSerializer<T extends TransactionStartedResponse>
      extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      TransactionStartedResponseParser.toJson(request, gen);
    }
  }

  static class TransactionStartedResponseDeserializer<T extends TransactionStartedResponse>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) TransactionStartedResponseParser.fromJson(jsonNode);
    }
  }
}
