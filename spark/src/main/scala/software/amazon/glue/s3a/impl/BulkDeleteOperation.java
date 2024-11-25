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

package software.amazon.glue.s3a.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.Retries;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.functional.Tuples;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.fs.BulkDeleteUtils.validatePathIsUnderParent;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * S3A Implementation of the {@link BulkDelete} interface.
 */
public class BulkDeleteOperation extends AbstractStoreOperation implements BulkDelete {

  private final BulkDeleteOperationCallbacks callbacks;

  private final Path basePath;

  private final int pageSize;

  public BulkDeleteOperation(
      final StoreContext storeContext,
      final BulkDeleteOperationCallbacks callbacks,
      final Path basePath,
      final int pageSize,
      final AuditSpan span) {
    super(storeContext, span);
    this.callbacks = requireNonNull(callbacks);
    this.basePath = requireNonNull(basePath);
    checkArgument(pageSize > 0, "Page size must be greater than 0");
    this.pageSize = pageSize;
  }

  @Override
  public int pageSize() {
    return pageSize;
  }

  @Override
  public Path basePath() {
    return basePath;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Map.Entry<Path, String>> bulkDelete(final Collection<Path> paths)
      throws IOException, IllegalArgumentException {
    requireNonNull(paths);
    checkArgument(paths.size() <= pageSize,
        "Number of paths (%d) is larger than the page size (%d)", paths.size(), pageSize);
    final StoreContext context = getStoreContext();
    final List<ObjectIdentifier> objects = paths.stream().map(p -> {
      checkArgument(p.isAbsolute(), "Path %s is not absolute", p);
      checkArgument(validatePathIsUnderParent(p, basePath),
              "Path %s is not under the base path %s", p, basePath);
      final String k = context.pathToKey(p);
      return ObjectIdentifier.builder().key(k).build();
    }).collect(toList());

    final List<Map.Entry<String, String>> errors = callbacks.bulkDelete(objects);
    if (!errors.isEmpty()) {

      final List<Map.Entry<Path, String>> outcomeElements = errors
          .stream()
          .map(error -> Tuples.pair(
              context.keyToPath(error.getKey()),
              error.getValue()
          ))
          .collect(toList());
      return outcomeElements;
    }
    return emptyList();
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Callbacks for the bulk delete operation.
   */
  public interface BulkDeleteOperationCallbacks {

    /**
     * Perform a bulk delete operation.
     * @param keys key list
     * @return paths which failed to delete (if any).
     * @throws IOException IO Exception.
     * @throws IllegalArgumentException illegal arguments
     */
    @Retries.RetryTranslated
    List<Map.Entry<String, String>> bulkDelete(final List<ObjectIdentifier> keys)
        throws IOException, IllegalArgumentException;
  }
}
