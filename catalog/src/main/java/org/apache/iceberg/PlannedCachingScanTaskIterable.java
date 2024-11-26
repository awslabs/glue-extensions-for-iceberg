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
package org.apache.iceberg;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.compress.utils.Sets;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

public class PlannedCachingScanTaskIterable implements CloseableIterable<FileScanTask> {
  private final List<FileScanTask> cachedResults = Lists.newArrayList();
  private final HashSet<String> dataFilePaths = Sets.newHashSet();
  private final AtomicBoolean isMaterialized;
  private final CachingIterator cachingIterator;

  public PlannedCachingScanTaskIterable(CloseableIterable<FileScanTask> delegate) {
    this.cachingIterator = new CachingIterator(delegate.iterator());
    this.isMaterialized = new AtomicBoolean(false);
  }

  @Override
  public synchronized CloseableIterator<FileScanTask> iterator() {
    if (isMaterialized.get()) {
      return CloseableIterator.withClose(cachedResults.iterator());
    }
    return cachingIterator;
  }

  public Set<String> getDataFilePaths() {
    return dataFilePaths;
  }

  public List<FileScanTask> getCachedResults() {
    return cachedResults;
  }

  public boolean isMaterialized() {
    return isMaterialized.get();
  }

  @Override
  public void close() throws IOException {}

  private class CachingIterator implements CloseableIterator<FileScanTask> {
    private final CloseableIterator<FileScanTask> delegate;

    public CachingIterator(CloseableIterator<FileScanTask> delegateIterator) {
      this.delegate = delegateIterator;
    }

    @Override
    public synchronized boolean hasNext() {
      if (!delegate.hasNext()) {
        isMaterialized.set(true);
        return false;
      }
      return true;
    }

    @Override
    public synchronized FileScanTask next() {
      FileScanTask fileScanTask = delegate.next();
      cachedResults.add(fileScanTask);
      dataFilePaths.add(fileScanTask.file().path().toString());
      return fileScanTask;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
