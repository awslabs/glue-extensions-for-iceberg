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

import java.util.Map;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.LocationUtil;

public class GlueTableLocationProvider implements LocationProvider {

  private static final String DELIMITER = "__GLUETEXT__";
  private static final Joiner JOINER = Joiner.on(DELIMITER).skipNulls();

  private final String tableLocation;
  private final String tablePath;
  private final String stagingLocation;
  private LocationProvider fallback;

  public GlueTableLocationProvider(String tableLocation, Map<String, String> tableProperties) {
    this(
        tableLocation,
        tableProperties.get(GlueTableProperties.WRITE_LOCATION_PROVIDER_REST_TABLE_PATH),
        tableProperties.get(GlueTableProperties.WRITE_LOCATION_PROVIDER_STAGING_LOCATION));
  }

  public GlueTableLocationProvider(String tableLocation, String tablePath, String stagingLocation) {
    this.tableLocation = LocationUtil.stripTrailingSlash(tableLocation);
    this.tablePath = tablePath;
    if (stagingLocation == null) {
      this.stagingLocation = null;
      try {
        this.fallback =
            LocationProviders.locationsFor(
                tableLocation,
                ImmutableMap.of(
                    TableProperties.WRITE_LOCATION_PROVIDER_IMPL,
                    "org.apache.iceberg.aws.s3.S3LocationProvider"));
      } catch (IllegalArgumentException e) {
        this.fallback =
            LocationProviders.locationsFor(
                tableLocation, ImmutableMap.of(TableProperties.OBJECT_STORE_ENABLED, "true"));
      }
    } else {
      this.stagingLocation = LocationUtil.stripTrailingSlash(stagingLocation);
      this.fallback = null;
    }
  }

  @Override
  public String newDataLocation(String filename) {
    if (fallback == null) {
      return createAnnotatedStagingLocation(stagingLocation, tablePath) + filename;
    }
    return fallback.newDataLocation(filename);
  }

  @Override
  public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
    return newDataLocation(filename);
  }

  public static String createAnnotatedStagingLocation(String stagingLocation, String tablePath) {
    return JOINER.join(stagingLocation, LocationUtil.stripTrailingSlash(tablePath), "/");
  }

  public static String tablePathFromAnnotatedStagingLocation(String annotatedPath) {
    // Split by the delimiter and extract the second part
    String[] parts = annotatedPath.split(DELIMITER);

    // Ensure the path length is exactly 3
    if (parts.length == 3) {
      // Return the table path (second part)
      return parts[1];
    }

    // Throw an exception or handle the error if the length is not exactly 3
    throw new IllegalArgumentException(
        "Invalid annotated path: expected 3 parts but found " + parts.length);
  }

  public static boolean isAnnotatedStagingLocation(String path) {
    return path != null && path.contains(DELIMITER);
  }
}
