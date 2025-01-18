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

import com.amazonaws.auth.AccountIdAware;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Adapts a V1 {@link AWSCredentialsProvider} to the V2 {@link AwsCredentialsProvider} interface.
 * Implements both interfaces so can be used with either the V1 or V2 AWS SDK.
 */
final class V1ToV2AwsCredentialProviderAdapter implements V1V2AwsCredentialProviderAdapter {

  private final AWSCredentialsProvider v1CredentialsProvider;

  private V1ToV2AwsCredentialProviderAdapter(AWSCredentialsProvider v1CredentialsProvider) {
    this.v1CredentialsProvider = v1CredentialsProvider;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AWSCredentials toAdapt = v1CredentialsProvider.getCredentials();

    String accountId = null;
    if (toAdapt instanceof AccountIdAware) {
      accountId = ((AccountIdAware) toAdapt).getAccountId();
    }

    if (toAdapt instanceof AWSSessionCredentials) {
      return AwsSessionCredentials.builder()
          .accessKeyId(toAdapt.getAWSAccessKeyId())
          .secretAccessKey(toAdapt.getAWSSecretKey())
          .sessionToken(((AWSSessionCredentials) toAdapt).getSessionToken())
          .accountId(accountId)
          .build();
    } else if (toAdapt instanceof AnonymousAWSCredentials) {
      return AnonymousCredentialsProvider.create().resolveCredentials();
    } else {
      return AwsBasicCredentials.builder()
          .accessKeyId(toAdapt.getAWSAccessKeyId())
          .secretAccessKey(toAdapt.getAWSSecretKey())
          .accountId(accountId)
          .build();
    }
  }

  @Override
  public AWSCredentials getCredentials() {
    return v1CredentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    v1CredentialsProvider.refresh();
  }

  /**
   * @param v1CredentialsProvider V1 credential provider to adapt.
   * @return A new instance of the credentials provider adapter.
   */
  static V1ToV2AwsCredentialProviderAdapter create(AWSCredentialsProvider v1CredentialsProvider) {
    return new V1ToV2AwsCredentialProviderAdapter(v1CredentialsProvider);
  }
}
