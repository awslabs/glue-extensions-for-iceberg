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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.LocationUtil;
import software.amazon.glue.exceptions.ErrorResponse;
import software.amazon.glue.requests.UpdateTableRequest;
import software.amazon.glue.responses.LoadCatalogResponse;
import software.amazon.glue.responses.LoadTableResponse;
import software.amazon.glue.responses.TransactionStartedResponse;

public class GlueExtensionsTableOperations implements TableOperations {

  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final String HOST_CONNECTOR_NAME = "Iceberg";

  enum UpdateType {
    CREATE,
    REPLACE,
    SIMPLE
  }

  private final TableIdentifier identifier;
  private final GlueExtensionsClient client;
  private final GlueExtensionsPaths paths;
  private final LoadCatalogResponse catalogResponse;
  private final GlueLoadTableConfig loadTableConfig;
  private final GlueExtensionsProperties properties;
  private final GlueExtensionsSessionProperties sessionProperties;
  private final String tablePath;
  private final String transactionStatusPath;
  private final long transactionStatusCheckIntervalMs;
  private final FileIO io;
  private final LocationProvider locationProvider;
  private final List<MetadataUpdate> createChanges;
  private final TableMetadata replaceBase;
  private UpdateType updateType;
  private TableMetadata current;

  GlueExtensionsTableOperations(
      TableIdentifier identifier,
      GlueExtensionsClient client,
      GlueExtensionsPaths paths,
      GlueExtensionsProperties properties,
      GlueExtensionsSessionProperties sessionProperties,
      LoadCatalogResponse catalogResponse,
      GlueLoadTableConfig loadTableConfig,
      TableMetadata current,
      FileIO io) {
    this(
        identifier,
        client,
        paths,
        properties,
        sessionProperties,
        catalogResponse,
        loadTableConfig,
        current,
        io,
        UpdateType.SIMPLE,
        Lists.newArrayList());
  }

  GlueExtensionsTableOperations(
      TableIdentifier identifier,
      GlueExtensionsClient client,
      GlueExtensionsPaths paths,
      GlueExtensionsProperties properties,
      GlueExtensionsSessionProperties sessionProperties,
      LoadCatalogResponse catalogResponse,
      GlueLoadTableConfig loadTableConfig,
      TableMetadata current,
      FileIO io,
      UpdateType updateType,
      List<MetadataUpdate> createChanges) {
    this.identifier = identifier;
    this.client = client;
    this.paths = paths;
    this.properties = properties;
    this.sessionProperties = sessionProperties;
    this.catalogResponse = catalogResponse;
    this.loadTableConfig = loadTableConfig;
    this.tablePath = paths.table(identifier);
    this.transactionStatusPath = paths.transactionStatus();
    this.transactionStatusCheckIntervalMs = properties.transactionStatusCheckIntervalMs();
    this.io = GlueUtil.configureGlueTableFileIO(properties, client, tablePath, loadTableConfig, io);
    this.locationProvider =
        GlueUtil.configureLocationProvider(
            properties, current.location(), tablePath, loadTableConfig);

    this.updateType = updateType;
    this.createChanges = createChanges;
    this.replaceBase = current;
    if (updateType == UpdateType.CREATE) {
      this.current = null;
    } else {
      this.current = current;
    }
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  public String redshiftTableIdentifier() {
    return redshiftTableIdentifier(
        catalogResponse.identifier(),
        catalogResponse.targetRedshiftCatalogIdentifier() != null,
        identifier.namespace().toString(),
        identifier.name());
  }

  public GlueExtensionsClient client() {
    return client;
  }

  public GlueExtensionsPaths paths() {
    return paths;
  }

  public String tablePath() {
    return tablePath;
  }

  public GlueLoadTableConfig loadTableConfig() {
    return loadTableConfig;
  }

  public GlueExtensionsProperties properties() {
    return properties;
  }

  public GlueExtensionsSessionProperties sessionProperties() {
    return sessionProperties;
  }

  public Map<String, String> redshiftConnectionConfigs() {
    return ImmutableMap.<String, String>builder()
        .put("host_connector", HOST_CONNECTOR_NAME)
        .put("dbtable", redshiftTableIdentifier())
        .put(
            "data_api_database",
            parentCatalogArn(
                catalogResponse.identifier(),
                catalogResponse.targetRedshiftCatalogIdentifier() != null))
        .put("aws_iam_role", loadTableConfig.stagingDataTransferRole())
        .put("tempdir", locationProvider.newDataLocation(""))
        .put("tempdir_region", client.endpoint().region())
        .put("debug", Boolean.toString(properties.debugEnabled()))
        .put("tempformat", "parquet")
        .build();
  }

  @VisibleForTesting
  static String redshiftTableIdentifier(
      String catalogArn, boolean isCatalogLinked, String databaseName, String tableName) {
    String[] catalogParts = catalogArn.split(":catalog/");
    Preconditions.checkArgument(catalogParts.length == 2, "Invalid catalog ARN: %s", catalogArn);

    // If there is a target redshift catalog, i.e, link container case,
    // the redshift identifier should be linkcontainer_catalog.account_id
    if (isCatalogLinked) {
      String linkContainerCatalogPart = catalogParts[1];
      String accountId = catalogArn.split(":")[4];
      Preconditions.checkArgument(
          accountId.matches("^\\d{12}$"), "Invalid catalog ARN: %s", catalogArn);
      return "\""
          + linkContainerCatalogPart
          + "@"
          + accountId
          + "\".\""
          + databaseName
          + "\".\""
          + tableName
          + "\"";
    }

    // Extract namespace and database
    String nsDbPart = catalogParts[1];
    String[] nsDbParts = nsDbPart.split("/");
    Preconditions.checkArgument(nsDbParts.length == 2, "Invalid nested catalog ID: %s", nsDbPart);

    // Namespace and DB extracted
    String namespace = nsDbParts[0];
    String db = nsDbParts[1];

    // Formulate dbtable string
    return "\"" + db + "@" + namespace + "\".\"" + databaseName + "\".\"" + tableName + "\"";
  }

  /**
   * Get parent catalog ARN: arn:aws:glue:us-east-1:451785580005:catalog/catalog1/dev to:
   * arn:aws:glue:us-east-1:451785580005:catalog/catalog1
   */
  @VisibleForTesting
  static String parentCatalogArn(String arn, boolean isCatalogLinked) {
    if (isCatalogLinked) {
      return arn;
    }

    // Find the last index of "/" to remove the last segment
    int lastSlashIndex = arn.lastIndexOf('/');

    // Extract the substring up to the last "/"
    return arn.substring(0, lastSlashIndex);
  }

  @Override
  public TableMetadata current() {
    return current;
  }

  @Override
  public TableMetadata refresh() {
    return updateCurrentMetadata(
        client.get(tablePath, LoadTableResponse.class, ErrorHandlers.tableErrorHandler()));
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Consumer<ErrorResponse> errorHandler;
    List<UpdateRequirement> requirements;
    List<MetadataUpdate> updates;
    switch (updateType) {
      case CREATE:
        Preconditions.checkState(
            base == null, "Invalid base metadata for create transaction, expected null: %s", base);
        updates =
            ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
        requirements = UpdateRequirements.forCreateTable(updates);
        errorHandler = ErrorHandlers.tableErrorHandler(); // throws NoSuchTableException
        break;

      case REPLACE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        updates =
            ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
        // use the original replace base metadata because the transaction will refresh
        requirements = UpdateRequirements.forReplaceTable(replaceBase, updates);
        errorHandler = ErrorHandlers.tableCommitHandler();
        break;

      case SIMPLE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        updates = metadata.changes();
        if (isUsingExtensions()) {
          // Assert UUID for all operations
          requirements = ImmutableList.of(new UpdateRequirement.AssertTableUUID(base.uuid()));
        } else {
          requirements = UpdateRequirements.forUpdateTable(base, updates);
        }
        errorHandler = ErrorHandlers.tableCommitHandler();
        break;

      default:
        throw new UnsupportedOperationException(
            String.format("Update type %s is not supported", updateType));
    }

    UpdateTableRequest request = new UpdateTableRequest(requirements, updates);
    commitUpdateWithTransaction(request, errorHandler);
    if (isUsingExtensions()) {
      // For extension tables, update current metadata with discarded changes to
      // maintain expected schema for transactions
      this.current = TableMetadata.buildFrom(metadata).discardChanges().build();
    } else {
      refresh();
    }
    // all future commits should be simple commits
    this.updateType = UpdateType.SIMPLE;
  }

  private boolean isUsingExtensions() {
    return current != null && GlueUtil.useExtensionsForIcebergTable(current.properties());
  }

  public void commitUpdateWithTransaction(
      UpdateTableRequest request, Consumer<ErrorResponse> errorHandler) {
    // the error handler will throw necessary exceptions like CommitFailedException and
    // UnknownCommitStateException
    // TODO: ensure that the HTTP client lib passes HTTP client errors to the error handler
    TransactionStartedResponse response =
        client.post(tablePath, request, TransactionStartedResponse.class, errorHandler);
    GlueUtil.checkTransactionStatusUntilFinished(
        client, transactionStatusPath, response.transaction(), transactionStatusCheckIntervalMs);
  }

  @Override
  public FileIO io() {
    return io;
  }

  private TableMetadata updateCurrentMetadata(LoadTableResponse response) {
    //  This method ensures that we always have the latest schema, even if other
    //  sessions have modified it.
    if (current == null
        || !Objects.equals(current.metadataFileLocation(), response.metadataLocation())
        || !current.schema().sameSchema(response.tableMetadata().schema())) {
      this.current = response.tableMetadata();
    }
    return current;
  }

  private static String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public LocationProvider locationProvider() {
    return locationProvider;
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return GlueExtensionsTableOperations.metadataFileLocation(uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return GlueExtensionsTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return GlueExtensionsTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return GlueExtensionsTableOperations.this.newSnapshotId();
      }
    };
  }
}
