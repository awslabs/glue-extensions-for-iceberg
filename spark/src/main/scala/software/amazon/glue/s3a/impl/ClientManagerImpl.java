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
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import software.amazon.glue.s3a.S3ClientFactory;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.LazyAutoCloseableReference;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static software.amazon.glue.s3a.Statistic.STORE_CLIENT_CREATION;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfOperation;
import static org.apache.hadoop.util.Preconditions.checkState;
import static org.apache.hadoop.util.functional.FutureIO.awaitAllFutures;

/**
 * Client manager for on-demand creation of S3 clients,
 * with parallelized close of them in {@link #close()}.
 * Updates {@link org.apache.hadoop.fs.s3a.Statistic#STORE_CLIENT_CREATION}
 * to track count and duration of client creation.
 */
public class ClientManagerImpl implements ClientManager {

  public static final Logger LOG = LoggerFactory.getLogger(ClientManagerImpl.class);

  /**
   * Client factory to invoke.
   */
  private final S3ClientFactory clientFactory;

  /**
   * Closed flag.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Parameters to create sync/async clients.
   */
  private final S3ClientFactory.S3ClientCreationParameters clientCreationParameters;

  /**
   * Duration tracker factory for creation.
   */
  private final DurationTrackerFactory durationTrackerFactory;

  /**
   * Core S3 client.
   */
  private final LazyAutoCloseableReference<S3Client> s3Client;

  /** Async client is used for transfer manager. */
  private final LazyAutoCloseableReference<S3AsyncClient> s3AsyncClient;

  /** Transfer manager. */
  private final LazyAutoCloseableReference<S3TransferManager> transferManager;

  /**
   * Constructor.
   * <p>
   * This does not create any clients.
   * <p>
   * It does disable noisy logging from the S3 Transfer Manager.
   * @param clientFactory client factory to invoke
   * @param clientCreationParameters creation parameters.
   * @param durationTrackerFactory duration tracker.
   */
  public ClientManagerImpl(
      final S3ClientFactory clientFactory,
      final S3ClientFactory.S3ClientCreationParameters clientCreationParameters,
      final DurationTrackerFactory durationTrackerFactory) {
    this.clientFactory = requireNonNull(clientFactory);
    this.clientCreationParameters = requireNonNull(clientCreationParameters);
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
    this.s3Client = new LazyAutoCloseableReference<>(createS3Client());
    this.s3AsyncClient = new LazyAutoCloseableReference<>(createAyncClient());
    this.transferManager = new LazyAutoCloseableReference<>(createTransferManager());

    // fix up SDK logging.
    AwsSdkWorkarounds.prepareLogging();
  }

  /**
   * Create the function to create the S3 client.
   * @return a callable which will create the client.
   */
  private CallableRaisingIOE<S3Client> createS3Client() {
    return trackDurationOfOperation(
        durationTrackerFactory,
        STORE_CLIENT_CREATION.getSymbol(),
        () -> clientFactory.createS3Client(getUri(), clientCreationParameters));
  }

  /**
   * Create the function to create the S3 Async client.
   * @return a callable which will create the client.
   */
  private CallableRaisingIOE<S3AsyncClient> createAyncClient() {
    return trackDurationOfOperation(
        durationTrackerFactory,
        STORE_CLIENT_CREATION.getSymbol(),
        () -> clientFactory.createS3AsyncClient(getUri(), clientCreationParameters));
  }

  /**
   * Create the function to create the Transfer Manager.
   * @return a callable which will create the component.
   */
  private CallableRaisingIOE<S3TransferManager> createTransferManager() {
    return () -> {
      final S3AsyncClient asyncClient = s3AsyncClient.eval();
      return trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3TransferManager(asyncClient));
    };
  }

  @Override
  public synchronized S3Client getOrCreateS3Client() throws IOException {
    checkNotClosed();
    return s3Client.eval();
  }

  /**
   * Get the S3Client, raising a failure to create as an UncheckedIOException.
   * @return the S3 client
   * @throws UncheckedIOException failure to create the client.
   */
  @Override
  public synchronized S3Client getOrCreateS3ClientUnchecked() throws UncheckedIOException {
    checkNotClosed();
    return s3Client.get();
  }

  @Override
  public synchronized S3AsyncClient getOrCreateAsyncClient() throws IOException {
    checkNotClosed();
    return s3AsyncClient.eval();
  }

  /**
   * Get the AsyncS3Client, raising a failure to create as an UncheckedIOException.
   * @return the S3 client
   * @throws UncheckedIOException failure to create the client.
   */
  @Override
  public synchronized S3Client getOrCreateAsyncS3ClientUnchecked() throws UncheckedIOException {
    checkNotClosed();
    return s3Client.get();
  }

  @Override
  public synchronized S3TransferManager getOrCreateTransferManager() throws IOException {
    checkNotClosed();
    return transferManager.eval();
  }

  /**
   * Check that the client manager is not closed.
   * @throws IllegalStateException if it is closed.
   */
  private void checkNotClosed() {
    checkState(!closed.get(), "Client manager is closed");
  }

  /**
   * Close() is synchronized to avoid race conditions between
   * slow client creation and this close operation.
   * <p>
   * The objects are all deleted in parallel
   */
  @Override
  public synchronized void close() {
    if (closed.getAndSet(true)) {
      // re-entrant close.
      return;
    }
    // queue the closures.
    List<Future<Object>> l = new ArrayList<>();
    l.add(closeAsync(transferManager));
    l.add(closeAsync(s3AsyncClient));
    l.add(closeAsync(s3Client));

    // once all are queued, await their completion
    // and swallow any exception.
    try {
      awaitAllFutures(l);
    } catch (Exception e) {
      // should never happen.
      LOG.warn("Exception in close", e);
    }
  }

  /**
   * Get the URI of the filesystem.
   * @return URI to use when creating clients.
   */
  public URI getUri() {
    return clientCreationParameters.getPathUri();
  }

  /**
   * Queue closing a closeable, logging any exception, and returning null
   * to use in when awaiting a result.
   * @param reference closeable.
   * @param <T> type of closeable
   * @return null
   */
  private <T extends AutoCloseable> CompletableFuture<Object> closeAsync(
      LazyAutoCloseableReference<T> reference) {
    if (!reference.isSet()) {
      // no-op
      return completedFuture(null);
    }
    return supplyAsync(() -> {
      try {
        reference.close();
      } catch (Exception e) {
        LOG.warn("Failed to close {}", reference, e);
      }
      return null;
    });
  }

  @Override
  public String toString() {
    return "ClientManagerImpl{" +
        "closed=" + closed.get() +
        ", s3Client=" + s3Client +
        ", s3AsyncClient=" + s3AsyncClient +
        ", transferManager=" + transferManager +
        '}';
  }
}
