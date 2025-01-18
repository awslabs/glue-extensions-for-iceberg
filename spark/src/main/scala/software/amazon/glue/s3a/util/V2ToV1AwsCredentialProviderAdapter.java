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

package software.amazon.glue.s3a.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Adapts a V1 {@link AWSCredentialsProvider} to the V2 {@link AwsCredentialsProvider} interface.
 * Implements both interfaces so can be used with either the V1 or V2 AWS SDK.
 */
final class V2ToV1AwsCredentialProviderAdapter implements V1V2AwsCredentialProviderAdapter {

  private final AwsCredentialsProvider v2CredentialsProvider;

  private V2ToV1AwsCredentialProviderAdapter(AwsCredentialsProvider v2CredentialsProvider) {
    this.v2CredentialsProvider = v2CredentialsProvider;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return v2CredentialsProvider.resolveCredentials();
  }

  @Override
  public AWSCredentials getCredentials() {
    if (v2CredentialsProvider instanceof AnonymousCredentialsProvider) {
      return new AnonymousAWSCredentials();
    }

    AwsCredentials toAdapt = v2CredentialsProvider.resolveCredentials();

    String accountId = null;
    if (toAdapt.accountId().isPresent()) {
      accountId = toAdapt.accountId().get();
    }

    if (toAdapt instanceof AwsSessionCredentials) {
      return new BasicSessionCredentials(
          toAdapt.accessKeyId(),
          toAdapt.secretAccessKey(),
          ((AwsSessionCredentials) toAdapt).sessionToken(),
          accountId);
    } else {
      return new BasicAWSCredentials(toAdapt.accessKeyId(), toAdapt.secretAccessKey(), accountId);
    }
  }

  @Override
  public void refresh() {
    // Refresh isn't defined for V2 so this is a no-op
  }

  /**
   * @param v2CredentialsProvider V2 credential provider to adapt.
   * @return A new instance of the credentials provider adapter.
   */
  static V2ToV1AwsCredentialProviderAdapter create(AwsCredentialsProvider v2CredentialsProvider) {
    return new V2ToV1AwsCredentialProviderAdapter(v2CredentialsProvider);
  }
}