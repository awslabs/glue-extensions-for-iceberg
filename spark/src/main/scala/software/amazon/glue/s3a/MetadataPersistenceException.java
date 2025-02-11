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

import org.apache.hadoop.fs.PathIOException;

/**
 * Indicates the metadata associated with the given Path could not be persisted
 * to the metadata store (e.g. S3Guard / DynamoDB).  When this occurs, the
 * file itself has been successfully written to S3, but the metadata may be out
 * of sync.  The metadata can be corrected with the "s3guard import" command
 * provided by {@link software.amazon.glue.s3a.s3guard.S3GuardTool}.
 */
public class MetadataPersistenceException extends PathIOException {

  /**
   * Constructs a MetadataPersistenceException.
   * @param path path of the affected file
   * @param cause cause of the issue
   */
  public MetadataPersistenceException(String path, Throwable cause) {
    super(path, cause);
  }
}
