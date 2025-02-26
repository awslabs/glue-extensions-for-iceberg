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

package software.amazon.glue.s3a.s3guard;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.S3AFileStatus;
import software.amazon.glue.s3a.S3ObjectAttributes;
import software.amazon.glue.s3a.impl.StoreContext;

/**
 * A no-op implementation of MetadataStore.  Clients that use this
 * implementation should behave the same as they would without any
 * MetadataStore.
 */
public class NullMetadataStore implements MetadataStore {

  @Override
  public void initialize(FileSystem fs, ITtlTimeProvider ttlTimeProvider)
      throws IOException {
  }

  @Override
  public void initialize(Configuration conf, ITtlTimeProvider ttlTimeProvider)
      throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void delete(Path path,
      final BulkOperationState operationState)
      throws IOException {
  }

  @Override
  public void forgetMetadata(Path path) throws IOException {
  }

  @Override
  public void deleteSubtree(Path path,
      final BulkOperationState operationState)
      throws IOException {
  }

  @Override
  public void deletePaths(final Collection<Path> paths,
      @Nullable final BulkOperationState operationState) throws IOException {

  }

  @Override
  public PathMetadata get(Path path) throws IOException {
    return null;
  }

  @Override
  public PathMetadata get(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException {
    return null;
  }

  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    return null;
  }

  @Override
  public void move(Collection<Path> pathsToDelete,
      Collection<PathMetadata> pathsToCreate,
      final BulkOperationState operationState) throws IOException {
  }

  @Override
  public void put(final PathMetadata meta) throws IOException {
  }

  @Override
  public void put(PathMetadata meta,
      final BulkOperationState operationState) throws IOException {
  }

  @Override
  public void put(Collection<? extends PathMetadata> meta,
      final BulkOperationState operationState) throws IOException {
  }

  @Override
  public void put(DirListingMetadata meta,
      final List<Path> unchangedEntries,
      final BulkOperationState operationState) throws IOException {
  }

  @Override
  public void destroy() throws IOException {
  }

  @Override
  public void prune(PruneMode pruneMode, long cutoff) {
  }

  @Override
  public long prune(PruneMode pruneMode, long cutoff, String keyPrefix) {
    return 0;
  }

  @Override
  public String toString() {
    return "NullMetadataStore";
  }

  @Override
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put("name", "Null Metadata Store");
    map.put("description", "This is not a real metadata store");
    return map;
  }

  @Override
  public void updateParameters(Map<String, String> parameters)
      throws IOException {
  }

  @Override
  public RenameTracker initiateRenameOperation(final StoreContext storeContext,
      final Path source,
      final S3AFileStatus sourceStatus,
      final Path dest)
      throws IOException {
    return new NullRenameTracker(storeContext, source, dest, this);
  }

  @Override
  public void setTtlTimeProvider(ITtlTimeProvider ttlTimeProvider) {
  }

  @Override
  public void addAncestors(final Path qualifiedPath,
      @Nullable final BulkOperationState operationState) throws IOException {
  }

  private static final class NullRenameTracker extends RenameTracker {

    private NullRenameTracker(
        final StoreContext storeContext,
        final Path source,
        final Path dest,
        MetadataStore metadataStore) {
      super("NullRenameTracker", storeContext, metadataStore, source, dest,
          null);
    }

    @Override
    public void fileCopied(final Path childSource,
        final S3ObjectAttributes sourceAttributes,
        final S3ObjectAttributes destAttributes,
        final Path destPath,
        final long blockSize,
        final boolean addAncestors) throws IOException {

    }

  }
}
