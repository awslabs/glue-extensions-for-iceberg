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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializationUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGlueTableLocationProvider {

  @Test
  public void testCreateAnnotatedStagingLocation() {
    // Define the values for mocks
    String stagingLocation =
        "s3://bucket-name/caller-id/read/123e4567-e89b-12d3-a456-426614174000/";
    String tablePath = "v1/catalogs/123456789012/namespaces/ns/tables/table";

    // Call the method to test
    String annotatedLocation =
        GlueTableLocationProvider.createAnnotatedStagingLocation(stagingLocation, tablePath);

    // Verify the result
    String expectedLocation = stagingLocation + "__GLUETEXT__" + tablePath + "__GLUETEXT__" + "/";
    assertEquals(expectedLocation, annotatedLocation);
  }

  @Test
  public void testGetTablePathFromAnnotatedStagingLocation() {
    // Define the annotated path
    String annotatedPath =
        "s3://bucket-name/caller-id/read/123e-e89-12d-a45/__GLUETEXT__v1/catalogs/123456789012/namespaces/ns/tables/table__GLUETEXT__/some/path/to/file.parquet";

    // Call the method to test
    String tablePath =
        GlueTableLocationProvider.tablePathFromAnnotatedStagingLocation(annotatedPath);

    // Verify the result
    assertEquals("v1/catalogs/123456789012/namespaces/ns/tables/table", tablePath);
  }

  @Test
  public void testGetTablePathFromAnnotatedStagingLocation_InvalidPathLength() {
    // Path with no delimiters:
    String annotatedPath = "s3://bucket-name/some/path/123e4567-e89b-12d3-a456-426614174000/";

    // Assert that an IllegalArgumentException is thrown
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          GlueTableLocationProvider.tablePathFromAnnotatedStagingLocation(annotatedPath);
        });
  }

  @Test
  public void testIsAnnotatedStagingLocation() {
    // Test with a valid annotated path
    String annotatedPath =
        "s3://bucket-name/caller-id/read/123e-e89-12d-a45/__GLUETEXT__v1/catalogs/123456789012/namespaces/ns/tables/table__GLUETEXT__/some/path/to/file.parquet";
    assertTrue(GlueTableLocationProvider.isAnnotatedStagingLocation(annotatedPath));

    // Test with a path that has no delimiter
    String nonAnnotatedPath = "s3://bucket-name/some/path/123e4567-e89b-12d3-a456-426614174000/";
    assertFalse(GlueTableLocationProvider.isAnnotatedStagingLocation(nonAnnotatedPath));
  }

  @Test
  public void testRoundtripGlueTableLocationProviderSerialization() {
    GlueTableLocationProvider locationProvider =
        new GlueTableLocationProvider("s3://table/location", "table/path", "s3://staging/location");
    byte[] bytes = SerializationUtil.serializeToBytes(locationProvider);
    SerializationUtil.deserializeFromBytes(bytes);
  }

  @Test
  public void testLocationProviderWithStagingLocation() {
    String tablePath = "table/path";
    String tableLocation = "s3://table/location";
    String stagingLocation = "s3://staging/location";
    GlueTableLocationProvider locationProvider =
        new GlueTableLocationProvider(tableLocation, tablePath, stagingLocation);
    String fileLocation = locationProvider.newDataLocation("test.csv");
    Assertions.assertThat(fileLocation)
        .contains(tablePath, stagingLocation)
        .doesNotContain(tableLocation);
    Assertions.assertThat(GlueTableLocationProvider.isAnnotatedStagingLocation(fileLocation))
        .isTrue();
    Assertions.assertThat(
            GlueTableLocationProvider.tablePathFromAnnotatedStagingLocation(fileLocation))
        .isEqualTo(tablePath);
  }

  @Test
  public void testLocationProviderWithoutStagingLocation() {
    String tablePath = "table/path";
    String tableLocation = "s3://table/location";
    GlueTableLocationProvider locationProvider =
        new GlueTableLocationProvider(tableLocation, tablePath, null);
    String fileLocation = locationProvider.newDataLocation("test.csv");
    Assertions.assertThat(fileLocation).contains(tableLocation).doesNotContain(tablePath);
    Assertions.assertThat(GlueTableLocationProvider.isAnnotatedStagingLocation(fileLocation))
        .isFalse();
    Assertions.assertThatThrownBy(
            () -> GlueTableLocationProvider.tablePathFromAnnotatedStagingLocation(fileLocation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid annotated path");
  }

  @Test
  public void testReflectionConstructor() {
    String tablePath = "table/path";
    String tableLocation = "s3://table/location";
    String stagingLocation = "s3://staging/location";
    LocationProvider locationProvider =
        LocationProviders.locationsFor(
            tableLocation,
            ImmutableMap.of(
                TableProperties.WRITE_LOCATION_PROVIDER_IMPL,
                    GlueTableLocationProvider.class.getName(),
                GlueTableProperties.WRITE_LOCATION_PROVIDER_REST_TABLE_PATH, tablePath,
                GlueTableProperties.WRITE_LOCATION_PROVIDER_STAGING_LOCATION, stagingLocation));

    Assertions.assertThat(locationProvider).isInstanceOf(GlueTableLocationProvider.class);
    String fileLocation = locationProvider.newDataLocation("test.csv");
    Assertions.assertThat(fileLocation)
        .contains(tablePath, stagingLocation)
        .doesNotContain(tableLocation);
    Assertions.assertThat(GlueTableLocationProvider.isAnnotatedStagingLocation(fileLocation))
        .isTrue();
    Assertions.assertThat(
            GlueTableLocationProvider.tablePathFromAnnotatedStagingLocation(fileLocation))
        .isEqualTo(tablePath);
  }
}
