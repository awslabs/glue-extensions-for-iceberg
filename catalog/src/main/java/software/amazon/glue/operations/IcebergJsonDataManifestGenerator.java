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
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class IcebergJsonDataManifestGenerator implements DataFileManifestGenerator {
  private final TableOperations ops;
  private final Map<Integer, PartitionSpec> specs;
  private static final String ICEBERG_MANIFEST_FILES_EXTENSION = ".icebergdatafiles.json";

  public IcebergJsonDataManifestGenerator(TableOperations ops, Map<Integer, PartitionSpec> specs) {
    this.ops = ops;
    this.specs = specs;
  }

  @Override
  public List<ManifestLocation> buildDataManifest(List<DataFile> dataFiles) {
    String manifestFileLocation = createManifestFileLocation();
    OutputFile outputFile = ops.io().newOutputFile(manifestFileLocation);
    IcebergDataManifestParser.writeDataFilesToJson(outputFile, dataFiles, specs);
    return ImmutableList.of(new ManifestLocation(manifestFileLocation, ManifestType.ICEBERG_JSON));
  }

  private String createManifestFileLocation() {
    String sessionId = UUID.randomUUID().toString().replace("-", "");
    return ops.locationProvider().newDataLocation(sessionId) + ICEBERG_MANIFEST_FILES_EXTENSION;
  }
}
