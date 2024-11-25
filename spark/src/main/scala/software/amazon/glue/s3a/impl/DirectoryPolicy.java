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

import org.apache.hadoop.fs.Path;

import static software.amazon.glue.s3a.Constants.DIRECTORY_MARKER_POLICY_AUTHORITATIVE;
import static software.amazon.glue.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static software.amazon.glue.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;

/**
 * Interface for Directory Marker policies to implement.
 */

public interface DirectoryPolicy {



  /**
   * Should a directory marker be retained?
   * @param path path a file/directory is being created with.
   * @return true if the marker MAY be kept, false if it MUST be deleted.
   */
  boolean keepDirectoryMarkers(Path path);

  /**
   * Get the marker policy.
   * @return policy.
   */
  MarkerPolicy getMarkerPolicy();

  /**
   * Describe the policy for marker tools and logs.
   * @return description of the current policy.
   */
  String describe();

  /**
   * Does a specific path have the relevant option.
   * This is to be forwarded from the S3AFileSystem.hasPathCapability
   * But only for those capabilities related to markers*
   * @param path path
   * @param capability capability
   * @return true if the capability is supported, false if not
   * @throws IllegalArgumentException if the capability is unknown.
   */
  boolean hasPathCapability(Path path, String capability);

  /**
   * Supported retention policies.
   */
  enum MarkerPolicy {

    /**
     * Delete markers.
     * <p>
     * This is the classic S3A policy,
     */
    Delete(DIRECTORY_MARKER_POLICY_DELETE),

    /**
     * Keep markers.
     * <p>
     * This is <i>Not backwards compatible</i>.
     */
    Keep(DIRECTORY_MARKER_POLICY_KEEP),

    /**
     * Keep markers in authoritative paths only.
     * <p>
     * This is <i>Not backwards compatible</i> within the
     * auth paths, but is outside these.
     */
    Authoritative(DIRECTORY_MARKER_POLICY_AUTHORITATIVE);

    /**
     * The name of the option as allowed in configuration files
     * and marker-aware tooling.
     */
    private final String optionName;

    MarkerPolicy(final String optionName) {
      this.optionName = optionName;
    }

    /**
     * Get the option name.
     * @return name of the option
     */
    public String getOptionName() {
      return optionName;
    }
  }
}
