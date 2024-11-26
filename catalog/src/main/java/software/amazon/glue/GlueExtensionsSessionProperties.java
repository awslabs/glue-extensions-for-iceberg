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
package software.amazon.glue;

import java.io.Serializable;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class GlueExtensionsSessionProperties implements Serializable {
  public static final String CREDENTIALS_AWS_ACCESS_KEY_ID = "aws.access-key-id";
  public static final String CREDENTIALS_AWS_SECRET_ACCESS_KEY = "aws.secret-access-key";
  public static final String CREDENTIALS_AWS_SESSION_TOKEN = "aws.session-token";

  private final String credentialsAwsAccessKeyId;
  private final String credentialsAwsSecretAccessKey;
  private final String credentialsAwsSessionToken;

  public GlueExtensionsSessionProperties(SessionCatalog.SessionContext context) {
    if (context.credentials() != null
        && context
            .credentials()
            .containsKey(GlueExtensionsSessionProperties.CREDENTIALS_AWS_ACCESS_KEY_ID)) {
      this.credentialsAwsAccessKeyId =
          context.credentials().get(GlueExtensionsSessionProperties.CREDENTIALS_AWS_ACCESS_KEY_ID);
      this.credentialsAwsSecretAccessKey =
          context
              .credentials()
              .get(GlueExtensionsSessionProperties.CREDENTIALS_AWS_SECRET_ACCESS_KEY);
      Preconditions.checkNotNull(
          credentialsAwsSecretAccessKey,
          "AWS secret access key must not be null when AWS access key ID is set in session context credentials");
      this.credentialsAwsSessionToken =
          context.credentials().get(GlueExtensionsSessionProperties.CREDENTIALS_AWS_SESSION_TOKEN);
      Preconditions.checkNotNull(
          credentialsAwsSessionToken,
          "AWS session token must not be null when AWS access key ID is set in session context credentials");
    } else {
      this.credentialsAwsAccessKeyId = null;
      this.credentialsAwsSecretAccessKey = null;
      this.credentialsAwsSessionToken = null;
    }
  }

  public AwsSessionCredentials awsSessionCredentials() {
    return AwsSessionCredentials.create(
        credentialsAwsAccessKeyId, credentialsAwsSecretAccessKey, credentialsAwsSessionToken);
  }

  public AwsCredentialsProvider credentialsProvider() {
    return credentialsAwsAccessKeyId != null
        ? StaticCredentialsProvider.create(awsSessionCredentials())
        : DefaultCredentialsProvider.create();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GlueExtensionsSessionProperties{");
    sb.append("credentialsAwsAccessKeyId='").append(credentialsAwsAccessKeyId).append('\'');
    sb.append(", credentialsAwsSecretAccessKey='")
        .append(credentialsAwsSecretAccessKey)
        .append('\'');
    sb.append(", credentialsAwsSessionToken='").append(credentialsAwsSessionToken).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
