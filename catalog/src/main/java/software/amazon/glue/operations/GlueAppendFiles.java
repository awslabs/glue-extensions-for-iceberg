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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class GlueAppendFiles extends GlueUpdateFilesOperation<AppendFiles> implements AppendFiles {

  public GlueAppendFiles(GlueTable table) {
    super(table);
  }

  @Override
  protected AppendFiles self() {
    return this;
  }

  @Override
  public AppendFiles appendFile(DataFile file) {
    return add(file);
  }

  @Override
  public AppendFiles appendManifest(ManifestFile manifest) {
    Preconditions.checkArgument(
        !manifest.hasExistingFiles(), "Cannot append manifest with existing files");
    Preconditions.checkArgument(
        !manifest.hasDeletedFiles(), "Cannot append manifest with deleted files");
    Preconditions.checkArgument(
        manifest.snapshotId() == null || manifest.snapshotId() == -1,
        "Snapshot id must be assigned during commit");
    Preconditions.checkArgument(
        manifest.sequenceNumber() == -1, "Sequence number must be assigned during commit");
    ManifestReader<DataFile> reader = ManifestFiles.read(manifest, ops.io());
    reader.forEach(this::appendFile);
    return this;
  }
}
