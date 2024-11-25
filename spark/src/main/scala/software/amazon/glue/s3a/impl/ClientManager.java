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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Interface for on-demand/async creation of AWS clients
 * and extension services.
 */
public interface ClientManager extends Closeable {

  /**
   * Get the transfer manager, creating it and any dependencies if needed.
   * @return a transfer manager
   * @throws IOException on any failure to create the manager
   */
  S3TransferManager getOrCreateTransferManager()
      throws IOException;

  /**
   * Get the S3Client, raising a failure to create as an IOException.
   * @return the S3 client
   * @throws IOException failure to create the client.
   */
  S3Client getOrCreateS3Client() throws IOException;

  /**
   * Get the S3Client, raising a failure to create as an UncheckedIOException.
   * @return the S3 client
   * @throws UncheckedIOException failure to create the client.
   */
  S3Client getOrCreateS3ClientUnchecked() throws UncheckedIOException;

  /**
   * Get the Async S3Client,raising a failure to create as an IOException.
   * @return the Async S3 client
   * @throws IOException failure to create the client.
   */
  S3AsyncClient getOrCreateAsyncClient() throws IOException;

  /**
   * Get the AsyncS3Client, raising a failure to create as an UncheckedIOException.
   * @return the S3 client
   * @throws UncheckedIOException failure to create the client.
   */
  S3Client getOrCreateAsyncS3ClientUnchecked() throws UncheckedIOException;

  /**
   * Close operation is required to not raise exceptions.
   */
  void close();
}
