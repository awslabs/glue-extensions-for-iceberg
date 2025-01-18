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

import com.amazonaws.auth.AWSCredentialsProvider;
import javax.annotation.Nullable;

/**
 * This interface returns an {@link AWSCredentialsProvider} given {@link S3Call}s.
 * <p>
 * This interface is intended to be implemented by users of this SDK to conform to their specific
 * use case. Implementation detail is entirely left to the user.
 * </p>
 *
 * @see AWSCredentialsProvider
 */
public interface S3CredentialsResolver {

  /**
   * Given a {@link S3Call}, returns an {@link AWSCredentialsProvider}
   * to be used for accessing those resources.
   * <p>
   * This function can return null. If null is returned, the default credentials provider will be
   * used.
   * </p>
   *
   * @param s3Call the actual S3 operation {@link S3Call} being performed
   * @return the {@link AWSCredentialsProvider} to use for this context, or null if there is none
   */
  @Nullable
  AWSCredentialsProvider resolve(S3Call s3Call);
}
