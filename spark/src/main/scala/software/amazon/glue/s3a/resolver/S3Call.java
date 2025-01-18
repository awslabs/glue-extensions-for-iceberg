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

package software.amazon.glue.s3a.resolver;

import java.util.Collection;
import java.util.List;

public interface S3Call {
  /**
   * Returns the bucket name which this S3 call will use.
   *
   * @return the bucket name
   */
  String getBucketName();

  /**
   * Returns a {@link Collection} of {@link S3Resource}s used by this S3Call.
   *
   * @return the {@link S3Resource}s required
   * @see S3Resource
   */
  Collection<S3Resource> getS3Resources();

  /**
   * Returns a list of {@link S3Request} used by this S3 call.
   * {@link S3Request} contains the request and action name.
   */
  List<S3Request> getS3Requests();
}
