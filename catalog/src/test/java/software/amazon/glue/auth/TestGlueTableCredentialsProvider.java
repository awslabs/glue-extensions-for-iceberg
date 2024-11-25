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
package software.amazon.glue.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.glue.GlueLoadTableConfig.SERVER_SIDE_SCAN_PLANNING_ENABLED;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_ACCESS_KEY_ID;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_DATA_TRANSFER_ROLE_ARN;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_EXPIRATION_MS;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_LOCATION;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_SECRET_ACCESS_KEY;
import static software.amazon.glue.GlueLoadTableConfig.STAGING_SESSION_TOKEN;
import static software.amazon.glue.auth.GlueTableCredentialsProvider.PREFETCH_TIME;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.ImmutableMap;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueLoadTableConfig;
import software.amazon.glue.GlueTestUtil;
import software.amazon.glue.responses.LoadTableResponse;

class TestGlueTableCredentialsProvider {

  private GlueExtensionsClient client;
  private static final String TABLE_PATH =
      "/catalogs/test-catalog/namespaces/test-namespace/tables/test-table";
  private static final Supplier<Map<String, String>> HEADERS = () -> ImmutableMap.of("a", "b");

  @BeforeEach
  public void setup() {
    client = Mockito.mock(GlueExtensionsClient.class);
  }

  @AfterEach
  public void reset() {
    Mockito.reset(client);
  }

  /** Tests a scenario that the initial credential is still fresh, so no refresh needed. */
  @Test
  public void testNoCredentialRefresh() {
    Clock clock = Clock.systemUTC();
    GlueLoadTableConfig loadTableConfig = GlueTestUtil.glueLoadTableConfig(clock);

    GlueTableCredentialsProvider provider =
        new GlueTableCredentialsProvider(client, TABLE_PATH, loadTableConfig);

    AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();

    Assertions.assertThat(credentials.accessKeyId())
        .isEqualTo(loadTableConfig.stagingAccessKeyId());
    Assertions.assertThat(credentials.secretAccessKey())
        .isEqualTo(loadTableConfig.stagingSecretAccessKey());
    Assertions.assertThat(credentials.sessionToken())
        .isEqualTo(loadTableConfig.stagingSessionToken());
    Assertions.assertThat(credentials.expirationTime().get())
        .isEqualTo(loadTableConfig.stagingExpirationMs());
    Mockito.verifyNoInteractions(client);
  }

  /** Tests a scenario that the initial credential is expired, so a refresh is needed. */
  @Test
  public void testCredentialRefresh() {
    Clock clock = Clock.systemUTC();
    Duration expirationDuration = Duration.ofHours(1);

    LoadTableResponse response =
        LoadTableResponse.builder()
            .withTableMetadata(mock(TableMetadata.class))
            .addConfig(SERVER_SIDE_SCAN_PLANNING_ENABLED, "true")
            .addConfig(STAGING_LOCATION, "staging-location")
            .addConfig(STAGING_ACCESS_KEY_ID, "test-access-key-id-2")
            .addConfig(STAGING_SECRET_ACCESS_KEY, "test-secret-access-key-2")
            .addConfig(STAGING_SESSION_TOKEN, "test-session-token-2")
            .addConfig(
                STAGING_EXPIRATION_MS,
                Long.toString(Instant.now(clock).plus(expirationDuration).toEpochMilli()))
            .addConfig(STAGING_DATA_TRANSFER_ROLE_ARN, "test-data-transfer-role-arn")
            .build();
    when(client.get(eq(TABLE_PATH), eq(LoadTableResponse.class), any())).thenReturn(response);

    GlueLoadTableConfig loadTableConfig = GlueTestUtil.glueLoadTableConfig(clock);

    GlueTableCredentialsProvider provider =
        new GlueTableCredentialsProvider(
            client,
            TABLE_PATH,
            loadTableConfig,
            Clock.offset(
                clock, expirationDuration.plus(PREFETCH_TIME).minus(Duration.ofMinutes(1))));
    AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();

    Assertions.assertThat(credentials.accessKeyId()).isEqualTo("test-access-key-id-2");
    Assertions.assertThat(credentials.secretAccessKey()).isEqualTo("test-secret-access-key-2");
    Assertions.assertThat(credentials.sessionToken()).isEqualTo("test-session-token-2");
    Assertions.assertThat(credentials.expirationTime())
        .isNotEqualTo(loadTableConfig.stagingExpirationMs());
  }
}
