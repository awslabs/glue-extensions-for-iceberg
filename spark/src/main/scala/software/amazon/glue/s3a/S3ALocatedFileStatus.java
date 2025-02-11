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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.LocatedFileStatus;

/**
 * {@link LocatedFileStatus} extended to also carry ETag and object version ID.
 */
public class S3ALocatedFileStatus extends LocatedFileStatus implements EtagSource {

  private static final long serialVersionUID = 3597192103662929338L;

  private final String eTag;
  private final String versionId;

  private final Tristate isEmptyDirectory;

  public S3ALocatedFileStatus(S3AFileStatus status, BlockLocation[] locations) {
    super(checkNotNull(status), locations);
    this.eTag = status.getEtag();
    this.versionId = status.getVersionId();
    isEmptyDirectory = status.isEmptyDirectory();
  }

  /**
   * @return the S3 object eTag when available, else null.
   * @deprecated use {@link EtagSource#getEtag()} for
   * public access.
   */
  @Deprecated
  public String getETag() {
    return getEtag();
  }

  @Override
  public String getEtag() {
    return eTag;
  }

  public String getVersionId() {
    return versionId;
  }

  // equals() and hashCode() overridden to avoid FindBugs warning.
  // Base implementation is equality on Path only, which is still appropriate.

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Generate an S3AFileStatus instance, including etag and
   * version ID, if present.
   * @return the S3A status.
   */
  public S3AFileStatus toS3AFileStatus() {
    return new S3AFileStatus(
        getPath(),
        isDirectory(),
        isEmptyDirectory,
        getLen(),
        getModificationTime(),
        getBlockSize(),
        getOwner(),
        getEtag(),
        getVersionId());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        super.toString());
    sb.append("[eTag='").
        append(eTag != null ? eTag : "")
        .append('\'');
    sb.append(", versionId='")
        .append(versionId != null ? versionId: "")
        .append('\'');
    sb.append(']');
    return sb.toString();
  }
}
