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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRedshiftFileManifestGenerator {
  private static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(2)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(2)
          .build();
  private static RedshiftFileManifestGenerator manifestGenerator;
  private static InMemoryFileIO inMemoryFileIO;
  @TempDir public static Path temp;

  @BeforeAll
  public static void setUp() {
    File warehouse = temp.toFile();

    inMemoryFileIO = new InMemoryFileIO();
    TableOperations mockOps = mock(TableOperations.class);
    LocationProvider locationProvider = mock(LocationProvider.class);

    when(locationProvider.newDataLocation(any()))
        .thenAnswer(invocation -> warehouse.getAbsolutePath() + "/" + UUID.randomUUID());
    when(mockOps.locationProvider()).thenReturn(locationProvider);
    when(mockOps.io()).thenReturn(inMemoryFileIO);

    manifestGenerator = new RedshiftFileManifestGenerator(mockOps);
  }

  @Test
  public void testBuildDataManifest() throws IOException {
    List<DataFile> dataFiles = ImmutableList.of(FILE_A, FILE_B);
    List<ManifestLocation> manifestLocations = manifestGenerator.buildDataManifest(dataFiles);
    String manifestLocation = manifestLocations.get(0).getLocation();

    assertTrue(
        inMemoryFileIO.fileExists(manifestLocation),
        "Manifest file should exist in InMemoryFileIO");
    assertTrue(
        manifestLocation.endsWith(RedshiftFileManifestGenerator.REDSHIFT_MANIFEST_FILE_EXTENSION),
        "Manifest file should have the correct extension");

    InputFile inputFile = inMemoryFileIO.newInputFile(manifestLocation);
    JsonNode manifestJson = JsonUtil.mapper().readTree(inputFile.newStream());
    JsonNode entries = manifestJson.get(RedshiftFileManifestGenerator.ENTRIES);

    validateManifestJson(manifestJson, 2);
    validateManifestEntry(entries.get(0), FILE_A.path().toString(), FILE_A.fileSizeInBytes());
    validateManifestEntry(entries.get(1), FILE_B.path().toString(), FILE_B.fileSizeInBytes());
  }

  @Test
  public void testBuildDataManifestWithEmptyList() throws IOException {
    List<DataFile> dataFiles = ImmutableList.of();
    List<ManifestLocation> manifestLocations = manifestGenerator.buildDataManifest(dataFiles);

    String manifestLocation = manifestLocations.get(0).getLocation();
    assertTrue(
        inMemoryFileIO.fileExists(manifestLocation),
        "Manifest file should exist in InMemoryFileIO");

    InputFile inputFile = inMemoryFileIO.newInputFile(manifestLocation);
    JsonNode manifestJson = JsonUtil.mapper().readTree(inputFile.newStream());

    validateManifestJson(manifestJson, 0);
  }

  private void validateManifestJson(JsonNode manifestJson, int expectedEntriesCount) {
    assertTrue(
        manifestJson.has(RedshiftFileManifestGenerator.ENTRIES),
        "Manifest should have entries field");
    JsonNode entries = manifestJson.get(RedshiftFileManifestGenerator.ENTRIES);
    assertTrue(entries.isArray(), "entries should be an array");
    assertEquals(
        expectedEntriesCount,
        entries.size(),
        "entries should have " + expectedEntriesCount + " entries");
  }

  private void validateManifestEntry(
      JsonNode entry, String expectedUrl, long expectedContentLength) {
    assertEquals(
        expectedUrl,
        entry.get(RedshiftFileManifestGenerator.URL).asText(),
        "URL should match the DataFile path");
    assertTrue(
        entry.get(RedshiftFileManifestGenerator.MANDATORY).asBoolean(),
        "mandatory field should be true");
    assertEquals(
        expectedContentLength,
        entry
            .get(RedshiftFileManifestGenerator.META)
            .get(RedshiftFileManifestGenerator.CONTENT_LENGTH)
            .asLong(),
        "content_length should match file size");
  }
}
