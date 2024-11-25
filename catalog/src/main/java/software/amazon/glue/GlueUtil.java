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

import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.glue.auth.GlueTableCredentialsProvider;
import software.amazon.glue.requests.CheckTransactionStatusRequest;
import software.amazon.glue.responses.CheckTransactionStatusResponse;
import software.amazon.glue.responses.CheckTransactionStatusResponse.Status;
import software.amazon.glue.responses.LoadCatalogResponse;

public class GlueUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GlueUtil.class);
  public static final Joiner SLASH = Joiner.on("/");

  // value for Glue StorageDescriptor InputFormat
  private static final String REDSHIFT_FORMAT = "RedshiftFormat";

  // value for Glue federated database connection name
  private static final String REDSHIFT_CONNECTION = "aws:redshift";
  private static final String NAMESPACE_TYPE = "type";
  private static final String NAMESPACE_TYPE_REDSHIFT = "redshift";
  private static final String ICEBERG_REST_SIGV4_ENABLED = "rest.sigv4-enabled";
  private static final String ICEBERG_REST_SIGNING_NAME = "rest.signing-name";
  private static final String ICEBERG_REST_SIGNER_REGION = "rest.signing-region";
  private static final String ICEBERG_REST_ACCESS_KEY_ID = "rest.access-key-id";
  private static final String ICEBERG_REST_SECRET_ACCESS_KEY = "rest.secret-access-key";
  private static final String ICEBERG_REST_SESSION_TOKEN = "rest.session-token";
  private static final String GLUE_PARAM_SCAN_PLANNING_ENABLED =
      "aws.server-side-capabilities.scan-planning";
  private static final String GLUE_PARAM_DATA_COMMIT_ENABLED =
      "aws.server-side-capabilities.data-commit";

  private GlueUtil() {}

  public static String stripTrailingSlash(String path) {
    if (path == null) {
      return null;
    }

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  private static final Joiner.MapJoiner FORM_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Splitter.MapSplitter FORM_SPLITTER =
      Splitter.on("&").withKeyValueSeparator("=");

  /**
   * Encodes a map of form data as application/x-www-form-urlencoded.
   *
   * <p>This encodes the form with pairs separated by &amp; and keys separated from values by =.
   *
   * @param formData a map of form data
   * @return a String of encoded form data
   */
  public static String encodeFormData(Map<?, ?> formData) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    formData.forEach(
        (key, value) ->
            builder.put(encodeString(String.valueOf(key)), encodeString(String.valueOf(value))));
    return FORM_JOINER.join(builder.build());
  }

  /**
   * Decodes a map of form data from application/x-www-form-urlencoded.
   *
   * <p>This decodes the form with pairs separated by &amp; and keys separated from values by =.
   *
   * @param formString a map of form data
   * @return a map of key/value form data
   */
  public static Map<String, String> decodeFormData(String formString) {
    return FORM_SPLITTER.split(formString).entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                e -> GlueUtil.decodeString(e.getKey()), e -> GlueUtil.decodeString(e.getValue())));
  }

  /**
   * Encodes a string using URL encoding
   *
   * <p>{@link #decodeString(String)} should be used to decode.
   *
   * @param toEncode string to encode
   * @return UTF-8 encoded string, suitable for use as a URL parameter
   */
  public static String encodeString(String toEncode) {
    Preconditions.checkArgument(toEncode != null, "Invalid string to encode: null");
    try {
      return URLEncoder.encode(toEncode, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new UncheckedIOException(
          String.format("Failed to URL encode '%s': UTF-8 encoding is not supported", toEncode), e);
    }
  }

  /**
   * Decodes a URL-encoded string.
   *
   * <p>See also {@link #encodeString(String)} for URL encoding.
   *
   * @param encoded a string to decode
   * @return a decoded string
   */
  public static String decodeString(String encoded) {
    Preconditions.checkArgument(encoded != null, "Invalid string to decode: null");
    try {
      return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new UncheckedIOException(
          String.format("Failed to URL decode '%s': UTF-8 encoding is not supported", encoded), e);
    }
  }

  /**
   * Returns a String representation of a namespace that is suitable for use in a URL / URI.
   *
   * <p>This function needs to be called when a namespace is used as a path variable (or query
   * parameter etc.), to format the namespace per the spec.
   *
   * <p>{@link #decodeNamespace} should be used to parse the namespace from a URL parameter.
   *
   * @param ns namespace to encode
   * @return UTF-8 encoded string representing the namespace, suitable for use as a URL parameter
   */
  public static String encodeNamespace(Namespace ns) {
    Preconditions.checkArgument(ns != null, "Invalid namespace: null");
    checkSingleLevelNamespace(ns);
    return encodeString(ns.level(0));
  }

  public static Namespace decodeNamespace(String encodedNs) {
    Preconditions.checkArgument(encodedNs != null, "Invalid namespace: null");
    return Namespace.of(decodeString(encodedNs));
  }

  public static void checkTransactionStatusUntilFinished(
      GlueExtensionsClient client, String path, String transaction, long sleepDurationMs) {

    CheckTransactionStatusRequest request =
        CheckTransactionStatusRequest.builder().withTransaction(transaction).build();

    boolean transactionFinished = false;
    while (!transactionFinished) {
      CheckTransactionStatusResponse response =
          client.post(
              path,
              request,
              CheckTransactionStatusResponse.class,
              ErrorHandlers.transactionErrorHandler());

      Status status = response.status();
      switch (status) {
        case FINISHED:
          transactionFinished = true;
          break;
        case FAILED:
          throw new RuntimeException(
              String.format("Transaction %s failed, error: %s", transaction, response.error()));
        case CANCELED:
          throw new RuntimeException(String.format("Transaction %s canceled", transaction));
        case STARTED:
          try {
            Thread.sleep(sleepDurationMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                String.format(
                    "Interrupted while polling status of transaction status for %s", transaction),
                e);
          }
          break;
        default:
          throw new RuntimeException(
              String.format("Invalid status of transaction %s: %s", transaction, status));
      }
    }
  }

  public static boolean useExtensionsForGlueDatabase(
      LoadCatalogResponse catalogResponse, Database database) {
    if (database == null) {
      LOG.info("Not use extensions: no database info in catalog {}", catalogResponse);
      return false;
    }

    if (catalogResponse != null && catalogResponse.useExtensions()) {
      LOG.info("Use extensions: database {} in catalog {}", database, catalogResponse);
      return true;
    }

    // fallback to ensure we use extensions for Redshift database
    if (database.federatedDatabase() != null) {
      if (REDSHIFT_CONNECTION.equalsIgnoreCase(database.federatedDatabase().connectionName())) {
        LOG.info("Use extensions: Redshift database {} in catalog {}", database, catalogResponse);
        return true;
      }
    }

    LOG.info("Not use extensions: database {} in catalog {}", database, catalogResponse);
    return false;
  }

  public static boolean useExtensionsForIcebergNamespace(Map<String, String> namespaceProperties) {
    if (namespaceProperties == null) {
      LOG.info("Not use extensions: no namespace properties");
      return false;
    }

    if (NAMESPACE_TYPE_REDSHIFT.equalsIgnoreCase(namespaceProperties.get(NAMESPACE_TYPE))) {
      LOG.info("Use extensions: namespace properties {}", namespaceProperties);
      return true;
    }

    LOG.info("Not use extensions: namespace properties {}", namespaceProperties);
    return false;
  }

  public static boolean useExtensionsForGlueTable(
      LoadCatalogResponse catalogResponse, Table table) {
    // catalog response must be available for read pushdown
    if (catalogResponse == null) {
      LOG.info("Not use extensions: no catalog info");
      return false;
    }

    if (table == null) {
      LOG.info("Not use extensions: no table info in catalog {}", catalogResponse);
      return false;
    }

    // fallback to ensure we use extensions for Redshift table
    if (table.storageDescriptor() != null) {
      if (REDSHIFT_FORMAT.equalsIgnoreCase(table.storageDescriptor().inputFormat())) {
        LOG.info("Use extensions: Redshift table {} in catalog {}", table, catalogResponse);
        return true;
      }
    }

    boolean scanPlanningEnabled =
        table.hasParameters()
            && PropertyUtil.propertyAsBoolean(
                table.parameters(), GLUE_PARAM_SCAN_PLANNING_ENABLED, false);

    boolean dataCommitEnabled =
        table.hasParameters()
            && PropertyUtil.propertyAsBoolean(
                table.parameters(), GLUE_PARAM_DATA_COMMIT_ENABLED, false);

    if (scanPlanningEnabled || dataCommitEnabled) {
      LOG.info(
          "Use extensions: table {} in catalog {} with scanPlanningEnabled={}, dataCommitEnabled={}",
          table,
          catalogResponse,
          scanPlanningEnabled,
          dataCommitEnabled);
      return true;
    }

    LOG.info(
        "Not use extensions: table {} in catalog {} with scanPlanningEnabled={}, dataCommitEnabled={}",
        table,
        catalogResponse,
        scanPlanningEnabled,
        dataCommitEnabled);
    return false;
  }

  public static boolean useExtensionsForIcebergTable(Map<String, String> tableProperties) {
    if (tableProperties == null) {
      LOG.info("Not use extensions: no table properties");
      return false;
    }

    if (GlueTableProperties.AWS_WRITE_FORMAT_RMS.equalsIgnoreCase(
        tableProperties.get(GlueTableProperties.AWS_WRITE_FORMAT))) {
      LOG.info("Use extensions: table properties {}", tableProperties);
      return true;
    }

    LOG.info("Not use extensions: table properties {}", tableProperties);
    return false;
  }

  public static String convertGlueCatalogIdToCatalogPath(String glueCatalogId) {
    if (glueCatalogId == null) {
      return ":";
    }

    return glueCatalogId.replace("/", ":");
  }

  public static void checkSingleLevelNamespace(Namespace namespace) {
    if (namespace.length() > 1) {
      throw new ValidationException("Multi-level namespace is not allowed: %s", namespace);
    }
  }

  public static void checkDelegateCatalogAvailable(Catalog catalog, String operation) {
    if (catalog == null) {
      throw new UnsupportedOperationException("Unsupported operation: " + operation);
    }
  }

  public static FileIO configureGlueTableFileIO(
      GlueExtensionsProperties properties,
      GlueExtensionsClient client,
      String tablePath,
      GlueLoadTableConfig loadTableConfig,
      FileIO providedIO) {
    if (providedIO != null) {
      return providedIO;
    }

    if (GlueExtensionsProperties.FILE_IO_IMPL_DEFAULT.equals(properties.fileIOImpl())) {
      return new S3FileIO(
          () ->
              S3Client.builder()
                  .region(Region.of(client.endpoint().region()))
                  .credentialsProvider(
                      new GlueTableCredentialsProvider(client, tablePath, loadTableConfig))
                  .build());
    }
    return CatalogUtil.loadFileIO(properties.fileIOImpl(), properties.fileIOProperties(), null);
  }

  public static LocationProvider configureLocationProvider(
      GlueExtensionsProperties properties,
      String tableLocation,
      String tablePath,
      GlueLoadTableConfig loadTableConfig) {
    if (properties.tableMetadataUseStagingLocation() && loadTableConfig.stagingLocation() != null) {
      return LocationProviders.locationsFor(
          loadTableConfig.stagingLocation(),
          ImmutableMap.of(TableProperties.OBJECT_STORE_ENABLED, "true"));
    }
    return new GlueTableLocationProvider(
        tableLocation, tablePath, loadTableConfig.stagingLocation());
  }
}
