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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.cache.CachedSupplier;
import software.amazon.awssdk.utils.cache.RefreshResult;
import software.amazon.glue.ErrorHandlers;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueLoadTableConfig;
import software.amazon.glue.responses.LoadTableResponse;

/**
 * An {@link AwsCredentialsProvider} for {@link GlueTable}. It takes the initial credential from
 * {@link GlueLoadTableConfig}. If the initial credential has expired, it will refresh the
 * credential by calling the REST endpoint.
 */
@ThreadSafe
public class GlueTableCredentialsProvider implements AwsCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableCredentialsProvider.class);

  private static final Duration STALE_TIME = Duration.ofMillis(1L);
  @VisibleForTesting static final Duration PREFETCH_TIME = Duration.ofMinutes(5);

  private final CachedSupplier<AwsCredentials> credentialsCachedSupplier;
  private final GlueExtensionsClient client;
  private final String tablePath;
  private final GlueLoadTableConfig loadTableConfig;
  /** The clock for manipulating the time for testing purpose. */
  private final Clock clock;

  public GlueTableCredentialsProvider(
      GlueExtensionsClient client, String tablePath, GlueLoadTableConfig loadTableConfig) {
    this(client, tablePath, loadTableConfig, Clock.systemUTC());
  }

  public GlueTableCredentialsProvider(
      GlueExtensionsClient client,
      String tablePath,
      GlueLoadTableConfig loadTableConfig,
      Clock clock) {
    this.clock = Preconditions.checkNotNull(clock, "Clock must not be null");
    this.client = Preconditions.checkNotNull(client, "Client must not  be null");
    this.tablePath = Preconditions.checkNotNull(tablePath, "Resource path must not be null");
    this.loadTableConfig =
        Preconditions.checkNotNull(loadTableConfig, "Table context must not be null");
    this.credentialsCachedSupplier =
        CachedSupplier.builder(this::updateCredentials).clock(clock).build();
  }

  /** Update AWS credential when the initial credential expires. */
  private RefreshResult<AwsCredentials> updateCredentials() {

    AwsCredentials newCredentials;
    if (loadTableConfig.stagingExpirationMs().isAfter(Instant.now(clock).plus(PREFETCH_TIME))) {
      newCredentials =
          new AwsSessionCredentials.Builder()
              .accessKeyId(loadTableConfig.stagingAccessKeyId())
              .secretAccessKey(loadTableConfig.stagingSecretAccessKey())
              .sessionToken(loadTableConfig.stagingSessionToken())
              .expirationTime(loadTableConfig.stagingExpirationMs())
              .build();
    } else {
      LOG.info("Refresh credentials for table: {}", tablePath);
      LoadTableResponse response =
          client.get(tablePath, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
      GlueLoadTableConfig loadTableConfig = new GlueLoadTableConfig(response.config());
      newCredentials =
          new AwsSessionCredentials.Builder()
              .accessKeyId(loadTableConfig.stagingAccessKeyId())
              .secretAccessKey(loadTableConfig.stagingSecretAccessKey())
              .sessionToken(loadTableConfig.stagingSessionToken())
              .expirationTime(loadTableConfig.stagingExpirationMs())
              .build();
    }

    return RefreshResult.builder(newCredentials)
        .staleTime(newCredentials.expirationTime().get().minus(STALE_TIME))
        .prefetchTime(newCredentials.expirationTime().get().minus(PREFETCH_TIME))
        .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialsCachedSupplier.get();
  }
}
