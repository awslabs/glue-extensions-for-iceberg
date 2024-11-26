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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class RedshiftFileManifestGenerator implements DataFileManifestGenerator {
  private final TableOperations ops;
  protected static final String ENTRIES = "entries";
  protected static final String URL = "url";
  protected static final String MANDATORY = "mandatory";
  protected static final String META = "meta";
  protected static final String CONTENT_LENGTH = "content_length";
  protected static final String REDSHIFT_MANIFEST_FILE_EXTENSION = ".redshiftmanifest.json";

  public RedshiftFileManifestGenerator(TableOperations ops) {
    this.ops = ops;
  }

  @Override
  public List<ManifestLocation> buildDataManifest(List<DataFile> dataFiles) {
    String manifestFileLocation = createManifestFileLocation();
    OutputFile outputFile = ops.io().newOutputFile(manifestFileLocation);

    try (JsonGenerator generator =
        JsonUtil.factory().createGenerator(outputFile.create(), JsonEncoding.UTF8)) {
      generator.writeStartObject();
      generator.writeArrayFieldStart(ENTRIES);
      for (DataFile dataFile : dataFiles) {
        generator.writeStartObject();
        generator.writeStringField(URL, dataFile.path().toString());
        generator.writeBooleanField(MANDATORY, true);
        generator.writeObjectFieldStart(META);
        generator.writeNumberField(CONTENT_LENGTH, dataFile.fileSizeInBytes());
        generator.writeEndObject();
        generator.writeEndObject();
      }
      generator.writeEndArray();
      generator.writeEndObject();
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate data manifest file", e);
    }
    return ImmutableList.of(new ManifestLocation(manifestFileLocation, ManifestType.REDSHIFT));
  }

  private String createManifestFileLocation() {
    String sessionId = UUID.randomUUID().toString().replace("-", "");
    return ops.locationProvider().newDataLocation(sessionId) + REDSHIFT_MANIFEST_FILE_EXTENSION;
  }
}
