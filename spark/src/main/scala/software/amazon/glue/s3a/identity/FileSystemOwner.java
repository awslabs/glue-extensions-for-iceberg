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

package software.amazon.glue.s3a.identity;

import java.io.IOException;

/**
 * This class acts as an interface through which S3AFileSystem can retrieve information about the
 * principal operating as the owner of this S3AFileSystem instance.
 */
public interface FileSystemOwner {

  /**
   * @return the fully qualified name of the principal
   */
  String getFullUserName();

  /**
   * @return just the login name of the principal
   */
  String getShortUserName();

  /**
   * @return the primary group
   */
  String getGroup() throws IOException;

  /**
   * @return a String array containing every group this principal belongs to
   */
  String[] getGroupNames();

}