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

import com.amazonaws.services.s3.transfer.Upload;

/**
 * Simple struct that contains information about a S3 upload.
 */
public class UploadInfo {
  private final Upload upload;
  private final long length;

  public UploadInfo(Upload upload, long length) {
    this.upload = upload;
    this.length = length;
  }

  public Upload getUpload() {
    return upload;
  }

  public long getLength() {
    return length;
  }

}
