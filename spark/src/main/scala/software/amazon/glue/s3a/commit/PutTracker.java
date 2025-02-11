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

package software.amazon.glue.s3a.commit;

import com.amazonaws.services.s3.model.PartETag;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Multipart put tracker.
 * Base class does nothing except declare that any
 * MPU must complete in the {@code close()} operation.
 *
 */
@InterfaceAudience.Private
public class PutTracker {

  /** The destination. */
  private final String destKey;

  /**
   * Instantiate.
   * @param destKey destination key
   */
  public PutTracker(String destKey) {
    this.destKey = destKey;
  }

  /**
   * Startup event.
   * @return true if the multipart should start immediately.
   * @throws IOException any IO problem.
   */
  public boolean initialize() throws IOException {
    return false;
  }

  /**
   * Flag to indicate that output is not immediately visible after the stream
   * is closed. Default: false.
   * @return true if the output's visibility will be delayed.
   */
  public boolean outputImmediatelyVisible() {
    return true;
  }

  /**
   * Callback when the upload is is about to complete.
   * @param uploadId Upload ID
   * @param parts list of parts
   * @param bytesWritten bytes written
   * @param iostatistics nullable IO statistics
   * @return true if the commit is to be initiated immediately.
   * False implies the output stream does not need to worry about
   * what happens.
   * @throws IOException I/O problem or validation failure.
   */
  public boolean aboutToComplete(String uploadId,
      List<PartETag> parts,
      long bytesWritten,
      final IOStatistics iostatistics)
      throws IOException {
    return true;
  }

  /**
   * get the destination key. The default implementation returns the
   * key passed in: there is no adjustment of the destination.
   * @return the destination to use in PUT requests.
   */
  public String getDestKey() {
    return destKey;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "DefaultPutTracker{");
    sb.append("destKey='").append(destKey).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
