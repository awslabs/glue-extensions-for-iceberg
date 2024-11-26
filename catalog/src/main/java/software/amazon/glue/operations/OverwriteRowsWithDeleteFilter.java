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

public class OverwriteRowsWithDeleteFilter implements MetadataUpdate {
  private final List<ManifestLocation> addedManifestLocations;
  private final String deleteFilter;

  public OverwriteRowsWithDeleteFilter(
      List<ManifestLocation> addedManifestLocations, String deleteFilter) {
    this.addedManifestLocations = addedManifestLocations;
    this.deleteFilter = deleteFilter;
  }

  public List<ManifestLocation> getAddedManifestLocations() {
    return addedManifestLocations;
  }

  public String getDeleteFilter() {
    return deleteFilter;
  }
}
