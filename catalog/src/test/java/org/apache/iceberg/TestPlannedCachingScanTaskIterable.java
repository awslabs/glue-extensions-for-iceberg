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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.glue.GlueTestUtil;

public class TestPlannedCachingScanTaskIterable {

  private CloseableIterable<FileScanTask> mockDelegate;
  private CloseableIterator<FileScanTask> mockIterator;
  private FileScanTask mockFileScanTask;

  @BeforeEach
  public void setup() {
    mockDelegate = Mockito.mock(CloseableIterable.class);
    mockIterator = Mockito.mock(CloseableIterator.class);
    mockFileScanTask = GlueTestUtil.createTestFileScanTask();

    when(mockDelegate.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(true, false);
    when(mockIterator.next()).thenReturn(mockFileScanTask);
  }

  @Test
  public void testIteratorMaterializesResults() {
    PlannedCachingScanTaskIterable cachingScanTask =
        new PlannedCachingScanTaskIterable(mockDelegate);

    // Initially, the task should not be materialized
    Assertions.assertThat(cachingScanTask.isMaterialized())
        .isFalse()
        .as("Initially, the task should not be materialized");

    CloseableIterator<FileScanTask> iterator = cachingScanTask.iterator();
    Assertions.assertThat(iterator.hasNext()).isTrue();
    FileScanTask result = iterator.next();
    iterator.hasNext(); // materialize

    List<FileScanTask> cachedResults = cachingScanTask.getCachedResults();
    Set<String> dataFilePaths = cachingScanTask.getDataFilePaths();

    Assertions.assertThat(result).isEqualTo(mockFileScanTask);
    Assertions.assertThat(cachedResults).hasSize(1).contains(mockFileScanTask);
    Assertions.assertThat(dataFilePaths)
        .hasSize(1)
        .contains(mockFileScanTask.file().path().toString());

    Assertions.assertThat(cachingScanTask.isMaterialized())
        .isTrue()
        .as("the task should now be materialized");
    Assertions.assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void testCachedIteratorUsesMaterializedResults() {
    PlannedCachingScanTaskIterable cachingScanTask =
        new PlannedCachingScanTaskIterable(mockDelegate);

    // materialize the task
    CloseableIterator<FileScanTask> iterator = cachingScanTask.iterator();
    iterator.next();
    iterator.hasNext();
    Assertions.assertThat(iterator.hasNext()).isFalse();
    Assertions.assertThat(cachingScanTask.isMaterialized()).isTrue();

    CloseableIterator<FileScanTask> cachedIterator = cachingScanTask.iterator();
    Assertions.assertThat(cachedIterator.hasNext()).isTrue();
    FileScanTask cachedResult = cachedIterator.next();
    Assertions.assertThat(cachedResult).isEqualTo(mockFileScanTask);

    verify(mockIterator, times(1).description("delegated iterator is only used for materializing"))
        .next();
  }

  @Test
  public void testEmptyResultsFromDelegate() {
    when(mockIterator.hasNext()).thenReturn(false);
    PlannedCachingScanTaskIterable cachingScanTask =
        new PlannedCachingScanTaskIterable(mockDelegate);

    CloseableIterator<FileScanTask> iterator = cachingScanTask.iterator();

    Assertions.assertThat(iterator.hasNext()).isFalse();
    Assertions.assertThat(cachingScanTask.getCachedResults()).isEmpty();
    Assertions.assertThat(cachingScanTask.getDataFilePaths()).isEmpty();
    Assertions.assertThat(cachingScanTask.isMaterialized()).isTrue();
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    FileScanTask mockFileScanTask1 = GlueTestUtil.createTestFileScanTask("1");
    FileScanTask mockFileScanTask2 = GlueTestUtil.createTestFileScanTask("2");
    CloseableIterable<FileScanTask> mockDelegate1 =
        CloseableIterable.withNoopClose(ImmutableList.of(mockFileScanTask1, mockFileScanTask2));

    PlannedCachingScanTaskIterable cachingIterable =
        new PlannedCachingScanTaskIterable(mockDelegate1);

    int numThreads = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> cachingIterable.iterator().forEachRemaining(System.out::println));
    }

    executorService.shutdown();
    Assertions.assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(cachingIterable.isMaterialized()).isTrue();
    Assertions.assertThat(cachingIterable.getDataFilePaths()).hasSize(2);
    Assertions.assertThat(cachingIterable.getCachedResults()).hasSize(2);
  }
}
