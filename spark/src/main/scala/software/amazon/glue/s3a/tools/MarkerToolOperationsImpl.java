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

package software.amazon.glue.s3a.tools;

import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import software.amazon.glue.s3a.S3AFileStatus;
import software.amazon.glue.s3a.impl.MultiObjectDeleteException;
import software.amazon.glue.s3a.impl.OperationCallbacks;


/**
 * Implement the marker tool operations by forwarding to the
 * {@link OperationCallbacks} instance provided in the constructor.
 */
public class MarkerToolOperationsImpl implements MarkerToolOperations {

  private final OperationCallbacks operationCallbacks;

  /**
   * Constructor.
   * @param operations implementation of the operations
   */
  public MarkerToolOperationsImpl(final OperationCallbacks operations) {
    this.operationCallbacks = operations;
  }

  @Override
  public RemoteIterator<S3AFileStatus> listObjects(final Path path,
      final String key)
      throws IOException {
    return operationCallbacks.listObjects(path, key);
  }

  @Override
  public void removeKeys(
      final List<ObjectIdentifier> keysToDelete,
      final boolean deleteFakeDir)
      throws MultiObjectDeleteException, AwsServiceException, IOException {
    operationCallbacks.removeKeys(keysToDelete, deleteFakeDir
    );
  }

}
