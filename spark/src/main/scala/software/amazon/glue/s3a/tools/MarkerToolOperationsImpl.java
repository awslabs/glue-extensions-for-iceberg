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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import software.amazon.glue.s3a.S3AFileStatus;
import software.amazon.glue.s3a.impl.OperationCallbacks;
import software.amazon.glue.s3a.s3guard.BulkOperationState;

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
  public DeleteObjectsResult removeKeys(
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final boolean deleteFakeDir,
      final List<Path> undeletedObjectsOnFailure,
      final BulkOperationState operationState,
      final boolean quiet)
      throws MultiObjectDeleteException, AmazonClientException, IOException {
    return operationCallbacks.removeKeys(keysToDelete, deleteFakeDir,
        undeletedObjectsOnFailure, operationState, quiet);
  }

}
