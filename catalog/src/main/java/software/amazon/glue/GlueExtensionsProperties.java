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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.utils.Sets;
import org.apache.iceberg.util.PropertyUtil;

public class GlueExtensionsProperties implements Serializable {

  // Note: Any new key should be added to GlueExtensionsProperties.CATALOG_PROPERTY_KEYS

  public static final String SESSION_CACHE_EXPIRATION_MS =
      "glue.extensions.session-cache.expiration-ms";
  public static final long SESSION_CACHE_EXPIRATION_MS_DEFAULT = TimeUnit.HOURS.toMillis(1);
  public static final String HTTP_CLIENT_MAX_CONNECTIONS_PER_ROUTE =
      "glue.extensions.http-client.connections-per-route";
  public static final int HTTP_CLIENT_MAX_CONNECTIONS_PER_ROUTE_DEFAULT = 100;
  public static final String HTTP_CLIENT_MAX_CONNECTIONS =
      "glue.extensions.http-client.max-connections";
  public static final int HTTP_CLIENT_MAX_CONNECTIONS_DEFAULT = 100;
  public static final String HTTP_CLIENT_MAX_RETRIES = "glue.extensions.http-client.max-retries";
  public static final int HTTP_CLIENT_MAX_RETRIES_DEFAULT = 5;
  public static final String HTTP_CLIENT_CONNECTION_TIMEOUT_MS =
      "glue.extensions.http-client.connection-timeout-ms";
  public static final long HTTP_CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT = 10000;
  public static final String HTTP_CLIENT_SOCKET_TIMEOUT_MS =
      "glue.extensions.http-client.socket-timeout-ms";
  public static final long HTTP_CLIENT_SOCKET_TIMEOUT_MS_DEFAULT = 60000;
  public static final String SCAN_PLANNING_ENABLED = "glue.extensions.scan-planning-enabled";
  public static final boolean SCAN_PLANNING_ENABLED_DEFAULT = false;
  public static final String SCAN_PLANNING_STATUS_CHECK_INTERVAL_MS =
      "glue.extensions.scan-planning.status-check-interval-ms";
  public static final int SCAN_PLANNING_STATUS_CHECK_INTERVAL_MS_DEFAULT = 1000;
  public static final String SCAN_PLANNING_CACHE_EXPIRATION_MS =
      "glue.extensions.scan-planning.cache-expiration-ms";
  public static final long TABLE_SCAN_CACHE_EXPIRATION_MS_DEFAULT = TimeUnit.MINUTES.toMillis(30);
  public static final String TRANSACTION_STATUS_CHECK_INTERVAL_MS =
      "glue.extensions.transaction.status-check-interval-ms";
  public static final long TRANSACTION_STATUS_CHECK_INTERVAL_MS_DEFAULT = 1000;
  public static final String DELEGATE_CATALOG_IMPL = "glue.extensions.delegate-catalog-impl";
  public static final String DELEGATE_CATALOG_PROPERTIES_PREFIX =
      "glue.extensions.delegate-catalog.";
  public static final String FILE_IO_IMPL = "glue.extensions.file-io-impl";
  public static final String FILE_IO_IMPL_DEFAULT = "org.apache.iceberg.aws.s3.S3FileIO";
  public static final String FILE_IO_PROPERTIES_PREFIX = "glue.extensions.file-io.";
  public static final String DATA_COMMIT_MANIFEST_TYPE =
      "glue.extensions.data-commit.manifest-type";
  public static final String DATA_COMMIT_MANIFEST_TYPE_DEFAULT = "redshift";
  public static final String TABLE_METADATA_USE_STAGING_LOCATION =
      "glue.extensions.table-metadata.use-staging-location";
  public static final boolean TABLE_METADATA_USE_STAGING_LOCATION_DEFAULT = false;
  public static final String TABLE_CACHE_FILE_SCAN_TASKS =
      "glue.extensions.table-cache-file-scan-tasks";
  public static final boolean TABLE_CACHE_FILE_SCAN_TASKS_DEFAULT = false;
  public static final String GLUE_CATALOG_ID = "glue.id";
  public static final String GLUE_EXTENSIONS_ENABLED = "glue.extensions-enabled";
  public static final boolean GLUE_EXTENSIONS_ENABLED_DEFAULT = true;
  public static final String GLUE_EXTENSIONS_DEBUG_ENABLED = "glue.extensions.debug-enabled";
  public static final boolean GLUE_EXTENSIONS_DEBUG_ENABLED_DEFAULT = false;

  // Any new key should be added here
  public static final Set<String> CATALOG_PROPERTY_KEYS =
      Sets.newHashSet(

          // GlueExtensionsProperties
          SESSION_CACHE_EXPIRATION_MS,
          HTTP_CLIENT_MAX_CONNECTIONS_PER_ROUTE,
          HTTP_CLIENT_MAX_CONNECTIONS,
          HTTP_CLIENT_MAX_RETRIES,
          HTTP_CLIENT_CONNECTION_TIMEOUT_MS,
          HTTP_CLIENT_SOCKET_TIMEOUT_MS,
          SCAN_PLANNING_ENABLED,
          SCAN_PLANNING_STATUS_CHECK_INTERVAL_MS,
          SCAN_PLANNING_CACHE_EXPIRATION_MS,
          TRANSACTION_STATUS_CHECK_INTERVAL_MS,
          TABLE_METADATA_USE_STAGING_LOCATION,
          DELEGATE_CATALOG_IMPL,
          DELEGATE_CATALOG_PROPERTIES_PREFIX,
          FILE_IO_IMPL,
          FILE_IO_PROPERTIES_PREFIX,
          DATA_COMMIT_MANIFEST_TYPE,
          GLUE_CATALOG_ID,
          GLUE_EXTENSIONS_ENABLED,

          // GlueExtensionsEndpoint
          GlueExtensionsEndpoint.GLUE_ENDPOINT,
          GlueExtensionsEndpoint.GLUE_EXTENSIONS_ENDPOINT,
          GlueExtensionsEndpoint.GLUE_REGION);

  private final long sessionCacheExpirationMs;
  private final int httpClientMaxConnectionsPerRoute;
  private final int httpClientMaxConnections;
  private final int httpClientMaxRetries;
  private final long httpClientConnectionTimeoutMs;
  private final long httpClientSocketTimeoutMs;
  private final boolean scanPlanningEnabled;
  private final long scanPlanningStatusCheckIntervalMs;
  private final long scanPlanningCacheExpirationMs;
  private final long transactionStatusCheckIntervalMs;
  private final boolean tableMetadataUseStagingLocation;
  private final boolean tableCacheFileScanTasks;
  private final String delegateCatalogImpl;
  private final Map<String, String> delegateCatalogProperties;
  private final String fileIOImpl;
  private final Map<String, String> fileIOProperties;
  private final String dataCommitManifestType;
  private final String glueCatalogId;
  private final boolean glueExtensionsEnabled;
  private final boolean debugEnabled;

  public GlueExtensionsProperties(Map<String, String> properties) {
    this.sessionCacheExpirationMs =
        PropertyUtil.propertyAsLong(
            properties, SESSION_CACHE_EXPIRATION_MS, SESSION_CACHE_EXPIRATION_MS_DEFAULT);
    this.httpClientMaxConnectionsPerRoute =
        PropertyUtil.propertyAsInt(
            properties,
            HTTP_CLIENT_MAX_CONNECTIONS_PER_ROUTE,
            HTTP_CLIENT_MAX_CONNECTIONS_PER_ROUTE_DEFAULT);
    this.httpClientMaxConnections =
        PropertyUtil.propertyAsInt(
            properties, HTTP_CLIENT_MAX_CONNECTIONS, HTTP_CLIENT_MAX_CONNECTIONS_DEFAULT);
    this.httpClientMaxRetries =
        PropertyUtil.propertyAsInt(
            properties, HTTP_CLIENT_MAX_RETRIES, HTTP_CLIENT_MAX_RETRIES_DEFAULT);
    this.httpClientConnectionTimeoutMs =
        PropertyUtil.propertyAsLong(
            properties,
            HTTP_CLIENT_CONNECTION_TIMEOUT_MS,
            HTTP_CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT);
    this.httpClientSocketTimeoutMs =
        PropertyUtil.propertyAsLong(
            properties, HTTP_CLIENT_SOCKET_TIMEOUT_MS, HTTP_CLIENT_SOCKET_TIMEOUT_MS_DEFAULT);
    this.scanPlanningEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, SCAN_PLANNING_ENABLED, SCAN_PLANNING_ENABLED_DEFAULT);
    this.scanPlanningStatusCheckIntervalMs =
        PropertyUtil.propertyAsLong(
            properties,
            SCAN_PLANNING_STATUS_CHECK_INTERVAL_MS,
            SCAN_PLANNING_STATUS_CHECK_INTERVAL_MS_DEFAULT);
    this.scanPlanningCacheExpirationMs =
        PropertyUtil.propertyAsLong(
            properties, SCAN_PLANNING_CACHE_EXPIRATION_MS, TABLE_SCAN_CACHE_EXPIRATION_MS_DEFAULT);
    this.transactionStatusCheckIntervalMs =
        PropertyUtil.propertyAsLong(
            properties,
            TRANSACTION_STATUS_CHECK_INTERVAL_MS,
            TRANSACTION_STATUS_CHECK_INTERVAL_MS_DEFAULT);
    this.tableMetadataUseStagingLocation =
        PropertyUtil.propertyAsBoolean(
            properties,
            TABLE_METADATA_USE_STAGING_LOCATION,
            TABLE_METADATA_USE_STAGING_LOCATION_DEFAULT);
    this.tableCacheFileScanTasks =
        PropertyUtil.propertyAsBoolean(
            properties, TABLE_CACHE_FILE_SCAN_TASKS, TABLE_CACHE_FILE_SCAN_TASKS_DEFAULT);
    this.delegateCatalogImpl = properties.get(DELEGATE_CATALOG_IMPL);
    this.delegateCatalogProperties =
        PropertyUtil.propertiesWithPrefix(properties, DELEGATE_CATALOG_PROPERTIES_PREFIX);
    this.fileIOImpl = PropertyUtil.propertyAsString(properties, FILE_IO_IMPL, FILE_IO_IMPL_DEFAULT);
    this.fileIOProperties =
        PropertyUtil.propertiesWithPrefix(properties, FILE_IO_PROPERTIES_PREFIX);
    this.dataCommitManifestType =
        PropertyUtil.propertyAsString(
            properties, DATA_COMMIT_MANIFEST_TYPE, DATA_COMMIT_MANIFEST_TYPE_DEFAULT);
    this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
    this.glueExtensionsEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_EXTENSIONS_ENABLED, GLUE_EXTENSIONS_ENABLED_DEFAULT);
    this.debugEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_EXTENSIONS_DEBUG_ENABLED, GLUE_EXTENSIONS_DEBUG_ENABLED_DEFAULT);
  }

  public long sessionCacheExpirationMs() {
    return sessionCacheExpirationMs;
  }

  public int httpClientMaxConnections() {
    return httpClientMaxConnections;
  }

  public int httpClientMaxConnectionsPerRoute() {
    return httpClientMaxConnectionsPerRoute;
  }

  public int httpClientMaxRetries() {
    return httpClientMaxRetries;
  }

  public long httpClientConnectionTimeoutMs() {
    return httpClientConnectionTimeoutMs;
  }

  public long httpClientSocketTimeoutMs() {
    return httpClientSocketTimeoutMs;
  }

  public boolean scanPlanningEnabled() {
    return scanPlanningEnabled;
  }

  public long scanPlanningStatusCheckIntervalMs() {
    return scanPlanningStatusCheckIntervalMs;
  }

  public long transactionStatusCheckIntervalMs() {
    return transactionStatusCheckIntervalMs;
  }

  public String delegateCatalogImpl() {
    return delegateCatalogImpl;
  }

  public Map<String, String> delegateCatalogProperties() {
    return delegateCatalogProperties;
  }

  public String fileIOImpl() {
    return fileIOImpl;
  }

  public Map<String, String> fileIOProperties() {
    return fileIOProperties;
  }

  public String dataCommitManifestType() {
    return dataCommitManifestType;
  }

  public long scanPlanningCacheExpirationMs() {
    return scanPlanningCacheExpirationMs;
  }

  public boolean tableMetadataUseStagingLocation() {
    return tableMetadataUseStagingLocation;
  }

  public boolean tableCacheFileScanTasks() {
    return tableCacheFileScanTasks;
  }

  public String glueCatalogId() {
    return glueCatalogId;
  }

  public boolean glueExtensionsEnabled() {
    return glueExtensionsEnabled;
  }

  public boolean debugEnabled() {
    return debugEnabled;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GlueExtensionsProperties{");
    sb.append("dataCommitManifestType='").append(dataCommitManifestType).append('\'');
    sb.append(", sessionCacheExpirationMs=").append(sessionCacheExpirationMs);
    sb.append(", httpClientMaxConnectionsPerRoute=").append(httpClientMaxConnectionsPerRoute);
    sb.append(", httpClientMaxConnections=").append(httpClientMaxConnections);
    sb.append(", httpClientMaxRetries=").append(httpClientMaxRetries);
    sb.append(", httpClientConnectionTimeoutMs=").append(httpClientConnectionTimeoutMs);
    sb.append(", httpClientSocketTimeoutMs=").append(httpClientSocketTimeoutMs);
    sb.append(", scanPlanningEnabled=").append(scanPlanningEnabled);
    sb.append(", scanPlanningStatusCheckIntervalMs=").append(scanPlanningStatusCheckIntervalMs);
    sb.append(", scanPlanningCacheExpirationMs=").append(scanPlanningCacheExpirationMs);
    sb.append(", transactionStatusCheckIntervalMs=").append(transactionStatusCheckIntervalMs);
    sb.append(", tableMetadataUseStagingLocation=").append(tableMetadataUseStagingLocation);
    sb.append(", tableCacheFileScanTasks=").append(tableCacheFileScanTasks);
    sb.append(", delegateCatalogImpl='").append(delegateCatalogImpl).append('\'');
    sb.append(", delegateCatalogProperties=").append(delegateCatalogProperties);
    sb.append(", fileIOImpl='").append(fileIOImpl).append('\'');
    sb.append(", fileIOProperties=").append(fileIOProperties);
    sb.append(", glueCatalogId='").append(glueCatalogId).append('\'');
    sb.append(", glueExtensionsEnabled=").append(glueExtensionsEnabled);
    sb.append(", debugEnabled=").append(debugEnabled);
    sb.append('}');
    return sb.toString();
  }
}
