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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.compress.utils.Sets;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import software.amazon.glue.ErrorHandlers;
import software.amazon.glue.GlueExtensionsTableOperations;
import software.amazon.glue.requests.UpdateTableRequest;

public abstract class GlueUpdateFilesOperation<ThisT> implements SnapshotUpdate<ThisT> {
  protected final TableOperations ops;
  protected final DataFileManifestGenerator dataManifestBuilder;
  protected final GlueTable table;
  protected final List<DataFile> addedDataFiles = Lists.newArrayList();
  protected final List<DataFile> removedDataFiles = Lists.newArrayList();
  protected final Set<String> deletedDataFilePaths = Sets.newHashSet();

  public GlueUpdateFilesOperation(GlueTable table) {
    this.ops = table.operations();
    this.dataManifestBuilder = table.manifestBuilder();
    this.table = table;
  }

  protected abstract ThisT self();

  public ThisT add(DataFile file) {
    addedDataFiles.add(file);
    return self();
  }

  public ThisT remove(DataFile file) {
    removedDataFiles.add(file);
    deletedDataFilePaths.add(file.path().toString());
    return self();
  }

  @Override
  public ThisT set(String property, String value) {
    // no-op SparkWrite will set commit properties
    return self();
  }

  @Override
  public ThisT deleteWith(Consumer<String> deleteFunc) {
    throw new UnsupportedOperationException("Setting delete callback function is not supported");
  }

  @Override
  public ThisT stageOnly() {
    throw new UnsupportedOperationException("Staging snapshot is not supported");
  }

  @Override
  public ThisT scanManifestsWith(ExecutorService executorService) {
    throw new UnsupportedOperationException("Setting manifest scanner is not supported");
  }

  @Override
  public Snapshot apply() {
    throw new UnsupportedOperationException("Applying changes to snapshot is not supported");
  }

  @Override
  public void commit() {
    if (hasChanges()) {
      List<ManifestLocation> addedManifestLocations = buildManifest(addedDataFiles);
      List<ManifestLocation> removedManifestLocations = buildManifest(removedDataFiles);

      OverwriteRowsWithManifest overwriteRowsWithManifest =
          new OverwriteRowsWithManifest(addedManifestLocations, removedManifestLocations);
      commitWithTransaction(overwriteRowsWithManifest);
    }
  }

  protected boolean hasChanges() {
    return !addedDataFiles.isEmpty() || !removedDataFiles.isEmpty();
  }

  protected void commitWithDeleteFilterRequest(Expression deleteExpr) {
    List<ManifestLocation> addedManifestLocations = buildManifest(addedDataFiles);

    String deleteFilter = ExpressionParser.toJson(deleteExpr);
    OverwriteRowsWithDeleteFilter overwriteRowsWithDeleteFilter =
        new OverwriteRowsWithDeleteFilter(addedManifestLocations, deleteFilter);
    commitWithTransaction(overwriteRowsWithDeleteFilter);
  }

  protected void commitWithTransaction(MetadataUpdate update) {
    List<UpdateRequirement> requirements = buildUpdateRequirements(ImmutableList.of(update));
    UpdateTableRequest request = new UpdateTableRequest(requirements, ImmutableList.of(update));
    ((GlueExtensionsTableOperations) ops)
        .commitUpdateWithTransaction(request, ErrorHandlers.tableCommitHandler());
    table.invalidateScanCache();
  }

  private List<UpdateRequirement> buildUpdateRequirements(List<MetadataUpdate> updates) {
    return UpdateRequirements.forUpdateTable(ops.current(), ImmutableList.copyOf(updates));
  }

  private List<ManifestLocation> buildManifest(List<DataFile> dataFiles) {
    return !dataFiles.isEmpty()
        ? dataManifestBuilder.buildDataManifest(dataFiles)
        : ImmutableList.of();
  }
}
