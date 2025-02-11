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

import static com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_PART_STARTED_EVENT;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.Upload;
import software.amazon.glue.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;

/**
 * Listener to progress from AWS regarding transfers.
 */
public class ProgressableProgressListener implements ProgressListener {
  private static final Logger LOG = S3AFileSystem.LOG;
  private final S3AFileSystem fs;
  private final String key;
  private final Progressable progress;
  private long lastBytesTransferred;
  private final Upload upload;

  /**
   * Instantiate.
   * @param fs filesystem: will be invoked with statistics updates
   * @param key key for the upload
   * @param upload source of events
   * @param progress optional callback for progress.
   */
  public ProgressableProgressListener(S3AFileSystem fs,
      String key,
      Upload upload,
      Progressable progress) {
    this.fs = fs;
    this.key = key;
    this.upload = upload;
    this.progress = progress;
    this.lastBytesTransferred = 0;
  }

  @Override
  public void progressChanged(ProgressEvent progressEvent) {
    if (progress != null) {
      progress.progress();
    }

    // There are 3 http ops here, but this should be close enough for now
    ProgressEventType pet = progressEvent.getEventType();
    if (pet == TRANSFER_PART_STARTED_EVENT ||
        pet == TRANSFER_COMPLETED_EVENT) {
      fs.incrementWriteOperations();
    }

    long transferred = upload.getProgress().getBytesTransferred();
    long delta = transferred - lastBytesTransferred;
    fs.incrementPutProgressStatistics(key, delta);
    lastBytesTransferred = transferred;
  }

  /**
   * Method to invoke after upload has completed.
   * This can handle race conditions in setup/teardown.
   * @return the number of bytes which were transferred after the notification
   */
  public long uploadCompleted() {
    long delta = upload.getProgress().getBytesTransferred() -
        lastBytesTransferred;
    if (delta > 0) {
      LOG.debug("S3A write delta changed after finished: {} bytes", delta);
      fs.incrementPutProgressStatistics(key, delta);
    }
    return delta;
  }

}
