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

import javax.annotation.Nullable;

/**
 * LocalMetadataEntry is used to store entries in the cache of
 * LocalMetadataStore. PathMetadata or dirListingMetadata can be null. The
 * entry is not immutable.
 */
public final class LocalMetadataEntry {
  @Nullable
  private PathMetadata pathMetadata;
  @Nullable
  private DirListingMetadata dirListingMetadata;

  LocalMetadataEntry() {
  }

  LocalMetadataEntry(PathMetadata pmd){
    pathMetadata = pmd;
    dirListingMetadata = null;
  }

  LocalMetadataEntry(DirListingMetadata dlm){
    pathMetadata = null;
    dirListingMetadata = dlm;
  }

  public PathMetadata getFileMeta() {
    return pathMetadata;
  }

  public DirListingMetadata getDirListingMeta() {
    return dirListingMetadata;
  }


  public boolean hasPathMeta() {
    return this.pathMetadata != null;
  }

  public boolean hasDirMeta() {
    return this.dirListingMetadata != null;
  }

  public void setPathMetadata(PathMetadata pathMetadata) {
    this.pathMetadata = pathMetadata;
  }

  public void setDirListingMetadata(DirListingMetadata dirListingMetadata) {
    this.dirListingMetadata = dirListingMetadata;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LocalMetadataEntry{");
    if(pathMetadata != null) {
      sb.append("pathMetadata=" + pathMetadata.getFileStatus().getPath());
    }
    if(dirListingMetadata != null){
      sb.append("; dirListingMetadata=" + dirListingMetadata.getPath());
    }
    sb.append("}");
    return sb.toString();
  }
}
