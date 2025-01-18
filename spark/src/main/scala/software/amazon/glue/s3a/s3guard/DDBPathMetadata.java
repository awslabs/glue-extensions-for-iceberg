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

package software.amazon.glue.s3a.s3guard;

import software.amazon.glue.s3a.S3AFileStatus;
import software.amazon.glue.s3a.Tristate;

/**
 * {@code DDBPathMetadata} wraps {@link PathMetadata} and adds the
 * isAuthoritativeDir flag to provide support for authoritative directory
 * listings in {@link DynamoDBMetadataStore}.
 */
public class DDBPathMetadata extends PathMetadata {

  private boolean isAuthoritativeDir;

  public DDBPathMetadata(PathMetadata pmd) {
    super(pmd.getFileStatus(), pmd.isEmptyDirectory(), pmd.isDeleted(),
        pmd.getLastUpdated());
    this.isAuthoritativeDir = false;
    this.setLastUpdated(pmd.getLastUpdated());
  }

  public DDBPathMetadata(S3AFileStatus fileStatus) {
    super(fileStatus);
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, long lastUpdated) {
    super(fileStatus, isEmptyDir, isDeleted, lastUpdated);
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, boolean isAuthoritativeDir, long lastUpdated) {
    super(fileStatus, isEmptyDir, isDeleted, lastUpdated);
    this.isAuthoritativeDir = isAuthoritativeDir;
  }

  public boolean isAuthoritativeDir() {
    return isAuthoritativeDir;
  }

  public void setAuthoritativeDir(boolean authoritativeDir) {
    isAuthoritativeDir = authoritativeDir;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  @Override public String toString() {
    return "DDBPathMetadata{" +
        "isAuthoritativeDir=" + isAuthoritativeDir +
        ", PathMetadata=" + super.toString() +
        '}';
  }
}
