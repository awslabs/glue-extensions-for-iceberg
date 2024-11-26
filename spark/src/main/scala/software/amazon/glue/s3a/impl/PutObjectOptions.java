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

package software.amazon.glue.s3a.impl;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Extensible structure for options when putting/writing objects.
 */
public final class PutObjectOptions {

  /**
   * Can the PUT operation skip marker deletion?
   */
  private final boolean keepMarkers;

  /**
   * Storage class, if not null.
   */
  private final String storageClass;

  /**
   * Headers; may be null.
   */
  private final Map<String, String> headers;

  /**
   * Constructor.
   * @param keepMarkers Can the PUT operation skip marker deletion?
   * @param storageClass Storage class, if not null.
   * @param headers Headers; may be null.
   */
  public PutObjectOptions(
      final boolean keepMarkers,
      @Nullable final String storageClass,
      @Nullable final Map<String, String> headers) {
    this.keepMarkers = keepMarkers;
    this.storageClass = storageClass;
    this.headers = headers;
  }

  /**
   * Get the marker retention flag.
   * @return true if markers are to be retained.
   */
  public boolean isKeepMarkers() {
    return keepMarkers;
  }

  /**
   * Headers for the put/post request.
   * @return headers or null.
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public String toString() {
    return "PutObjectOptions{" +
        "keepMarkers=" + keepMarkers +
        ", storageClass='" + storageClass + '\'' +
        '}';
  }

  private static final PutObjectOptions KEEP_DIRS = new PutObjectOptions(true,
      null, null);
  private static final PutObjectOptions DELETE_DIRS = new PutObjectOptions(false,
      null, null);

  /**
   * Get the options to keep directories.
   * @return an instance which keeps dirs
   */
  public static PutObjectOptions keepingDirs() {
    return KEEP_DIRS;
  }

  /**
   * Get the options to delete directory markers.
   * @return an instance which deletes dirs
   */
  public static PutObjectOptions deletingDirs() {
    return DELETE_DIRS;
  }

}