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

package software.amazon.glue.s3a;

import software.amazon.awssdk.transfer.s3.model.ObjectTransfer;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;


/**
 * Listener to progress from AWS regarding transfers.
 */
public class ProgressableProgressListener implements TransferListener {
  private static final Logger LOG = S3AFileSystem.LOG;
  private final S3AStore store;
  private final String key;
  private final Progressable progress;
  private long lastBytesTransferred;

  /**
   * Instantiate.
   * @param store store: will be invoked with statistics updates
   * @param key key for the upload
   * @param progress optional callback for progress.
   */
  public ProgressableProgressListener(S3AStore store,
      String key,
      Progressable progress) {
    this.store = store;
    this.key = key;
    this.progress = progress;
    this.lastBytesTransferred = 0;
  }

  @Override
  public void transferInitiated(Context.TransferInitiated context) {
    store.incrementWriteOperations();
  }

  @Override
  public void transferComplete(Context.TransferComplete context) {
    store.incrementWriteOperations();
  }

  @Override
  public void bytesTransferred(Context.BytesTransferred context) {

    if(progress != null) {
      progress.progress();
    }

    long transferred = context.progressSnapshot().transferredBytes();
    long delta = transferred - lastBytesTransferred;
    store.incrementPutProgressStatistics(key, delta);
    lastBytesTransferred = transferred;
  }

  /**
   * Method to invoke after upload has completed.
   * This can handle race conditions in setup/teardown.
   * @param upload upload which has just completed.
   * @return the number of bytes which were transferred after the notification
   */
  public long uploadCompleted(ObjectTransfer upload) {

    long delta =
        upload.progress().snapshot().transferredBytes() - lastBytesTransferred;
    if (delta > 0) {
      LOG.debug("S3A write delta changed after finished: {} bytes", delta);
      store.incrementPutProgressStatistics(key, delta);
    }
    return delta;
  }

}
