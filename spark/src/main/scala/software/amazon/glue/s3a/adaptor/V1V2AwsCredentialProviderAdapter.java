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

package software.amazon.glue.s3a.adaptor;

import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Interface for a two-way adapter from the AWS SDK V1 {@link AWSCredentialsProvider} interface and
 * the AWS SDK V2 {@link AwsCredentialsProvider} interface.
 */
public interface V1V2AwsCredentialProviderAdapter
    extends AWSCredentialsProvider, AwsCredentialsProvider {

  /**
   * Creates a two-way adapter from a V1 {@link AWSCredentialsProvider} interface.
   *
   * @param v1CredentialsProvider V1 credentials provider.
   * @return Two-way credential provider adapter.
   */
  static V1V2AwsCredentialProviderAdapter adapt(AWSCredentialsProvider v1CredentialsProvider) {
    return V1ToV2AwsCredentialProviderAdapter.create(v1CredentialsProvider);
  }

  /**
   * Creates a two-way adapter from a V2 {@link AwsCredentialsProvider} interface.
   *
   * @param v2CredentialsProvider V2 credentials provider.
   * @return Two-way credential provider adapter.
   */
  static V1V2AwsCredentialProviderAdapter adapt(AwsCredentialsProvider v2CredentialsProvider) {
    return V2ToV1AwsCredentialProviderAdapter.create(v2CredentialsProvider);
  }
}
