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
import java.io.UncheckedIOException;
import java.util.Iterator;
import software.amazon.glue.s3a.Invoker;
import software.amazon.glue.s3a.Retries;

/**
 * A collection which wraps the result of a query or scan
 * with retries.
 * Important: iterate through this only once; the outcome
 * of repeating an iteration is "undefined"
 * @param <T> type of outcome.
 */
class RetryingCollection<T> implements Iterable<T> {

  /**
   * Source iterable.
   */
  private final Iterable<T> source;

  /**
   * Invoker for retries.
   */
  private final Invoker invoker;

  /**
   * Operation name for invoker.retry messages.
   */
  private final String operation;

  /**
   * Constructor.
   * @param operation Operation name for invoker.retry messages.
   * @param invoker Invoker for retries.
   * @param source Source iterable.
   */
  RetryingCollection(
      final String operation,
      final Invoker invoker,
      final Iterable<T> source) {
    this.operation = operation;
    this.source = source;
    this.invoker = invoker;
  }

  /**
   * Demand creates a new iterator which will retry all hasNext/next
   * operations through the invoker supplied in the constructor.
   * @return a new iterator.
   */
  @Override
  public Iterator<T> iterator() {
    return new RetryingIterator(source.iterator());
  }

  /**
   * An iterator which wraps a non-retrying iterator of scan results
   * (i.e {@code S3GuardTableAccess.DDBPathMetadataIterator}.
   */
  private final class RetryingIterator implements Iterator<T> {

    private final Iterator<T> iterator;

    private RetryingIterator(final Iterator<T> iterator) {
      this.iterator = iterator;
    }

    /**
     * {@inheritDoc}.
     * @throws UncheckedIOException for IO failure, including throttling.
     */
    @Override
    @Retries.RetryTranslated
    public boolean hasNext() {
      try {
        return invoker.retry(
            operation,
            null,
            true,
            iterator::hasNext);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * {@inheritDoc}.
     * @throws UncheckedIOException for IO failure, including throttling.
     */
    @Override
    @Retries.RetryTranslated
    public T next() {
      try {
        return invoker.retry(
            "Scan Dynamo",
            null,
            true,
            iterator::next);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

}
