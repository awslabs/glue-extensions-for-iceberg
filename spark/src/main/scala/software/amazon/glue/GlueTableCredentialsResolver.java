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

import static software.amazon.glue.GlueExtensionsEndpoint.GLUE_ENDPOINT;
import static software.amazon.glue.GlueExtensionsEndpoint.GLUE_REGION;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.SessionCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.glue.auth.GlueTableCredentialsProvider;
import software.amazon.glue.responses.LoadTableResponse;
import software.amazon.glue.s3a.resolver.S3Call;
import software.amazon.glue.s3a.resolver.S3CredentialsResolver;
import software.amazon.glue.s3a.resolver.S3Resource;
import software.amazon.glue.s3a.adaptor.V1V2AwsCredentialProviderAdapter;

public class GlueTableCredentialsResolver implements S3CredentialsResolver {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableCredentialsResolver.class);

  public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
  public static final String AWS_REGION = "aws.region";
  public static final long CACHE_EXPIRATION_MS = TimeUnit.HOURS.toMillis(1);
  private static final SessionCatalog.SessionContext EMPTY_SESSION =
      SessionCatalog.SessionContext.createEmpty();
  private static final Map<String, String> HADOOP_ICEBERG_CONFIG_MAP =
      ImmutableMap.of(
          AWS_GLUE_ENDPOINT, GLUE_ENDPOINT,
          AWS_REGION, GLUE_REGION);

  private GlueExtensionsClient client;
  private final Cache<String, GlueTableCredentialsProvider> credsProviderCache;

  public GlueTableCredentialsResolver(
      Configuration configuration, UserGroupInformation userGroupInformation) {

    Map<String, String> properties = hadoopConfigToMap(configuration);
    GlueExtensionsProperties extensionsProperties = new GlueExtensionsProperties(properties);
    GlueExtensionsEndpoint endpoint = GlueExtensionsEndpoint.from(properties);
    GlueExtensionsSessionProperties sessionProperties =
        new GlueExtensionsSessionProperties(EMPTY_SESSION);

    this.client =
        GlueExtensionsClient.builder()
            .withEndpoint(endpoint)
            .withProperties(extensionsProperties)
            .withSessionProperties(sessionProperties)
            .build();

    this.credsProviderCache =
        Caffeine.newBuilder().expireAfterAccess(Duration.ofMillis(CACHE_EXPIRATION_MS)).build();
  }

  public void setGlueExtensionsClient(GlueExtensionsClient client) {
    this.client = client;
  }

  @Override
  public AWSCredentialsProvider resolve(S3Call s3Call) {
    Collection<S3Resource> s3Resources = s3Call.getS3Resources();
    for (S3Resource resource : s3Resources) {
      String s3Path = resource.getPath();
      if (GlueTableLocationProvider.isAnnotatedStagingLocation(s3Path)) {
        String tablePath = GlueTableLocationProvider.tablePathFromAnnotatedStagingLocation(s3Path);

        GlueTableCredentialsProvider glueTableCredentialsProvider = credsProviderCache.get(
                tablePath,
                key -> {
                  LOG.info("Initializing credentials provider for table {}", tablePath);
                  LoadTableResponse response =
                          this.client.get(
                                  tablePath, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
                  return new GlueTableCredentialsProvider(
                          client, key, new GlueLoadTableConfig(response.config()));
                });

        return V1V2AwsCredentialProviderAdapter.adapt(glueTableCredentialsProvider);
      }
    }

    // Based on EMR-FS docs, this function can return null.
    // If null is returned, the default credentials provider will be used.
    return null;
  }

  private Map<String, String> hadoopConfigToMap(Configuration config) {
    Map<String, String> configMap = new HashMap<>();

    // Iterate over the entries in the Hadoop Configuration
    for (Map.Entry<String, String> entry : config) {
      // Only add to the map if the key exists in CATALOG_PROPERTY_KEYS
      if (GlueExtensionsProperties.CATALOG_PROPERTY_KEYS.contains(entry.getKey())) {
        configMap.put(entry.getKey(), entry.getValue());
      } else if (HADOOP_ICEBERG_CONFIG_MAP.containsKey(entry.getKey())) {
        // Map hadoop aws properties to Iceberg properties
        configMap.put(HADOOP_ICEBERG_CONFIG_MAP.get(entry.getKey()), entry.getValue());
      }
    }

    return configMap;
  }
}
