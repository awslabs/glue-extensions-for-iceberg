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

import java.util.List;
import org.apache.iceberg.MetadataUpdate;

public class OverwriteRowsWithManifest implements MetadataUpdate {
  private final List<ManifestLocation> addedManifestLocations;
  private final List<ManifestLocation> removedManifestLocations;

  public OverwriteRowsWithManifest(
      List<ManifestLocation> addedManifestLocations,
      List<ManifestLocation> removedManifestLocations) {
    this.addedManifestLocations = addedManifestLocations;
    this.removedManifestLocations = removedManifestLocations;
  }

  public List<ManifestLocation> getAddedManifestLocations() {
    return addedManifestLocations;
  }

  public List<ManifestLocation> getRemovedManifestLocations() {
    return removedManifestLocations;
  }
}
