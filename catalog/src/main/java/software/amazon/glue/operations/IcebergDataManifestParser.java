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
package software.amazon.glue.operations;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.util.JsonUtil;

public class IcebergDataManifestParser {
  private static final String SPEC_ID = "spec-id";
  private static final String CONTENT = "content";
  private static final String FILE_PATH = "file-path";
  private static final String FILE_FORMAT = "file-format";
  private static final String PARTITION = "partition";
  private static final String RECORD_COUNT = "record-count";
  private static final String FILE_SIZE = "file-size-in-bytes";
  private static final String COLUMN_SIZES = "column-sizes";
  private static final String VALUE_COUNTS = "value-counts";
  private static final String NULL_VALUE_COUNTS = "null-value-counts";
  private static final String NAN_VALUE_COUNTS = "nan-value-counts";
  private static final String LOWER_BOUNDS = "lower-bounds";
  private static final String UPPER_BOUNDS = "upper-bounds";
  private static final String KEY_METADATA = "key-metadata";
  private static final String SPLIT_OFFSETS = "split-offsets";
  private static final String EQUALITY_IDS = "equality-ids";
  private static final String SORT_ORDER_ID = "sort-order-id";
  private static final String CONTENT_FILES = "content-files";

  public static void writeDataFilesToJson(
      OutputFile outputFile, List<DataFile> dataFiles, Map<Integer, PartitionSpec> specs) {
    try (JsonGenerator generator = JsonUtil.factory().createGenerator(outputFile.create())) {
      generator.writeStartObject();
      generator.writeArrayFieldStart(CONTENT_FILES);
      for (DataFile dataFile : dataFiles) {
        Preconditions.checkArgument(
            specs.containsKey(dataFile.specId()), "Datafile referencing invalid partition spec");
        writeDataFileToJson(generator, dataFile, specs.get(dataFile.specId()));
      }
      generator.writeEndArray();
      generator.writeEndObject();
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate data manifest file", e);
    }
  }

  private static void writeDataFileToJson(
      JsonGenerator generator, DataFile dataFile, PartitionSpec spec) throws IOException {
    Preconditions.checkArgument(dataFile != null, "Invalid content file: null");
    Preconditions.checkArgument(spec != null, "Invalid partition spec: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");
    Preconditions.checkArgument(
        dataFile.specId() == spec.specId(),
        "Invalid partition spec id from content file: expected = %s, actual = %s",
        spec.specId(),
        dataFile.specId());
    Preconditions.checkArgument(
        spec.isPartitioned() == hasPartitionData(dataFile.partition()),
        "Invalid partition data from content file: expected = %s, actual = %s",
        spec.isPartitioned() ? "partitioned" : "unpartitioned",
        hasPartitionData(dataFile.partition()) ? "partitioned" : "unpartitioned");
    generator.writeStartObject();
    generator.writeNumberField(SPEC_ID, dataFile.specId());
    generator.writeStringField(CONTENT, dataFile.content().name());
    generator.writeStringField(FILE_PATH, dataFile.path().toString());
    generator.writeStringField(FILE_FORMAT, dataFile.format().name());
    if (dataFile.partition() != null) {
      generator.writeFieldName(PARTITION);
      SingleValueParser.toJson(spec.partitionType(), dataFile.partition(), generator);
    }
    generator.writeNumberField(FILE_SIZE, dataFile.fileSizeInBytes());
    metricsToJson(dataFile, generator);
    if (dataFile.keyMetadata() != null) {
      generator.writeFieldName(KEY_METADATA);
      SingleValueParser.toJson(DataFile.KEY_METADATA.type(), dataFile.keyMetadata(), generator);
    }

    if (dataFile.splitOffsets() != null) {
      JsonUtil.writeLongArray(SPLIT_OFFSETS, dataFile.splitOffsets(), generator);
    }

    if (dataFile.equalityFieldIds() != null) {
      JsonUtil.writeIntegerArray(EQUALITY_IDS, dataFile.equalityFieldIds(), generator);
    }

    if (dataFile.sortOrderId() != null) {
      generator.writeNumberField(SORT_ORDER_ID, dataFile.sortOrderId());
    }
    generator.writeEndObject();
  }

  public static List<DataFile> readDataFilesFromJson(
      InputFile inputFile, Map<Integer, PartitionSpec> specs) throws IOException {
    List<DataFile> dataFiles = Lists.newArrayList();
    try (InputStream inputStream = inputFile.newStream()) {
      String value =
          CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      JsonNode rootNode = JsonUtil.mapper().readTree(value);
      JsonNode contentFilesNode = rootNode.get(CONTENT_FILES);
      if (contentFilesNode != null && contentFilesNode.isArray()) {
        for (JsonNode dataFileNode : contentFilesNode) {
          dataFiles.add(parseDataFileFromJson(dataFileNode, specs));
        }
      }
    }
    return dataFiles;
  }

  public static DataFile parseDataFileFromJson(
      JsonNode jsonNode, Map<Integer, PartitionSpec> specs) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for content file: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for content file: non-object (%s)", jsonNode);

    int specId = JsonUtil.getInt(SPEC_ID, jsonNode);
    PartitionSpec spec = specs.get(specId);
    String filePath = JsonUtil.getString(FILE_PATH, jsonNode);
    FileFormat fileFormat = FileFormat.fromString(JsonUtil.getString(FILE_FORMAT, jsonNode));

    PartitionData partitionData = null;
    if (jsonNode.has(PARTITION)) {
      partitionData = new PartitionData(spec.partitionType());
      StructLike structLike =
          (StructLike) SingleValueParser.fromJson(spec.partitionType(), jsonNode.get(PARTITION));
      Preconditions.checkState(
          partitionData.size() == structLike.size(),
          "Invalid partition data size: expected = %s, actual = %s",
          partitionData.size(),
          structLike.size());
      for (int pos = 0; pos < partitionData.size(); ++pos) {
        Class<?> javaClass = spec.partitionType().fields().get(pos).type().typeId().javaClass();
        partitionData.set(pos, structLike.get(pos, javaClass));
      }
    }

    long fileSizeInBytes = JsonUtil.getLong(FILE_SIZE, jsonNode);
    Metrics metrics = metricsFromJson(jsonNode);
    ByteBuffer keyMetadata = JsonUtil.getByteBufferOrNull(KEY_METADATA, jsonNode);
    List<Long> splitOffsets = JsonUtil.getLongListOrNull(SPLIT_OFFSETS, jsonNode);
    return DataFiles.builder(spec)
        .withPath(filePath)
        .withFormat(fileFormat)
        .withPartition(partitionData)
        .withFileSizeInBytes(fileSizeInBytes)
        .withMetrics(metrics)
        .withSplitOffsets(splitOffsets)
        .withEncryptionKeyMetadata(keyMetadata)
        .withSplitOffsets(splitOffsets)
        .build();
  }

  private static void metricsToJson(ContentFile<?> contentFile, JsonGenerator generator)
      throws IOException {
    generator.writeNumberField(RECORD_COUNT, contentFile.recordCount());

    if (contentFile.columnSizes() != null) {
      generator.writeFieldName(COLUMN_SIZES);
      SingleValueParser.toJson(DataFile.COLUMN_SIZES.type(), contentFile.columnSizes(), generator);
    }

    if (contentFile.valueCounts() != null) {
      generator.writeFieldName(VALUE_COUNTS);
      SingleValueParser.toJson(DataFile.VALUE_COUNTS.type(), contentFile.valueCounts(), generator);
    }

    if (contentFile.nullValueCounts() != null) {
      generator.writeFieldName(NULL_VALUE_COUNTS);
      SingleValueParser.toJson(
          DataFile.NULL_VALUE_COUNTS.type(), contentFile.nullValueCounts(), generator);
    }

    if (contentFile.nullValueCounts() != null) {
      generator.writeFieldName(NAN_VALUE_COUNTS);
      SingleValueParser.toJson(
          DataFile.NAN_VALUE_COUNTS.type(), contentFile.nanValueCounts(), generator);
    }

    if (contentFile.lowerBounds() != null) {
      generator.writeFieldName(LOWER_BOUNDS);
      SingleValueParser.toJson(DataFile.LOWER_BOUNDS.type(), contentFile.lowerBounds(), generator);
    }

    if (contentFile.upperBounds() != null) {
      generator.writeFieldName(UPPER_BOUNDS);
      SingleValueParser.toJson(DataFile.UPPER_BOUNDS.type(), contentFile.upperBounds(), generator);
    }
  }

  private static Metrics metricsFromJson(JsonNode jsonNode) {
    long recordCount = JsonUtil.getLong(RECORD_COUNT, jsonNode);

    Map<Integer, Long> columnSizes = null;
    if (jsonNode.has(COLUMN_SIZES)) {
      columnSizes =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(DataFile.COLUMN_SIZES.type(), jsonNode.get(COLUMN_SIZES));
    }

    Map<Integer, Long> valueCounts = null;
    if (jsonNode.has(VALUE_COUNTS)) {
      valueCounts =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(DataFile.VALUE_COUNTS.type(), jsonNode.get(VALUE_COUNTS));
    }

    Map<Integer, Long> nullValueCounts = null;
    if (jsonNode.has(NULL_VALUE_COUNTS)) {
      nullValueCounts =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(
                  DataFile.NULL_VALUE_COUNTS.type(), jsonNode.get(NULL_VALUE_COUNTS));
    }

    Map<Integer, Long> nanValueCounts = null;
    if (jsonNode.has(NAN_VALUE_COUNTS)) {
      nanValueCounts =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(
                  DataFile.NAN_VALUE_COUNTS.type(), jsonNode.get(NAN_VALUE_COUNTS));
    }

    Map<Integer, ByteBuffer> lowerBounds = null;
    if (jsonNode.has(LOWER_BOUNDS)) {
      lowerBounds =
          (Map<Integer, ByteBuffer>)
              SingleValueParser.fromJson(DataFile.LOWER_BOUNDS.type(), jsonNode.get(LOWER_BOUNDS));
    }

    Map<Integer, ByteBuffer> upperBounds = null;
    if (jsonNode.has(UPPER_BOUNDS)) {
      upperBounds =
          (Map<Integer, ByteBuffer>)
              SingleValueParser.fromJson(DataFile.UPPER_BOUNDS.type(), jsonNode.get(UPPER_BOUNDS));
    }

    return new Metrics(
        recordCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds);
  }

  private static boolean hasPartitionData(StructLike partitionData) {
    return partitionData != null && partitionData.size() > 0;
  }
}
