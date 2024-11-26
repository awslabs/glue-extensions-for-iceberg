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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.MetadataUpdate.UpgradeFormatVersion;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import software.amazon.glue.operations.IcebergDataManifestParser;
import software.amazon.glue.operations.ManifestLocation;
import software.amazon.glue.operations.OverwriteRowsWithDeleteFilter;
import software.amazon.glue.operations.OverwriteRowsWithManifest;
import software.amazon.glue.operations.RenameTable;
import software.amazon.glue.requests.CreateNamespaceRequest;
import software.amazon.glue.requests.CreateTableRequest;
import software.amazon.glue.requests.UpdateTableRequest;
import software.amazon.glue.responses.CreateNamespaceResponse;
import software.amazon.glue.responses.LoadTableResponse;

public class CatalogHandlers {
  private static final Schema EMPTY_SCHEMA = new Schema();
  private static final String INTIAL_PAGE_TOKEN = "";

  private CatalogHandlers() {}

  /**
   * Exception used to avoid retrying commits when assertions fail.
   *
   * <p>When an assertion fails, it will throw CommitFailedException to send back to the client. But
   * the assertion checks happen in the block that is retried if {@link
   * TableOperations#commit(TableMetadata, TableMetadata)} throws CommitFailedException. This is
   * used to avoid retries for assertion failures, which are unwrapped and rethrown outside of the
   * commit loop.
   */
  private static class ValidationFailureException extends RuntimeException {
    private final CommitFailedException wrapped;

    private ValidationFailureException(CommitFailedException cause) {
      super(cause);
      this.wrapped = cause;
    }

    public CommitFailedException wrapped() {
      return wrapped;
    }
  }

  public static CreateNamespaceResponse createNamespace(
      SupportsNamespaces catalog, CreateNamespaceRequest request) {
    Namespace namespace = request.namespace();
    catalog.createNamespace(namespace, request.properties());
    return CreateNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(catalog.loadNamespaceMetadata(namespace))
        .build();
  }

  public static void dropNamespace(SupportsNamespaces catalog, Namespace namespace) {
    boolean dropped = catalog.dropNamespace(namespace);
    if (!dropped) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public static LoadTableResponse stageTableCreate(
      Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (catalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(request.properties());

    String location;
    if (request.location() != null) {
      location = request.location();
    } else {
      location =
          catalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table()
              .location();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            location,
            properties);

    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  public static LoadTableResponse createTable(
      Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table =
        catalog
            .buildTable(ident, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(request.properties())
            .create();

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static void dropTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident, false);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public static void purgeTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident, true);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public static LoadTableResponse updateTable(
      Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    TableMetadata finalMetadata;
    if (isCreate(request)) {
      // this is a hacky way to get TableOperations for an uncommitted table
      Transaction transaction =
          catalog.buildTable(ident, EMPTY_SCHEMA).createOrReplaceTransaction();
      if (transaction instanceof BaseTransaction) {
        BaseTransaction baseTransaction = (BaseTransaction) transaction;
        finalMetadata = create(baseTransaction.underlyingOps(), request);
      } else {
        throw new IllegalStateException(
            "Cannot wrap catalog that does not produce BaseTransaction");
      }

    } else {
      Table table = catalog.loadTable(ident);
      if (table instanceof BaseTable) {
        BaseTable baseTable = (BaseTable) table;
        if (request.updates().get(0) instanceof RenameTable) {
          return handleRenameUpdate(catalog, ident, request, baseTable);
        } else if (request.updates().get(0) instanceof OverwriteRowsWithManifest) {
          return handleOverwriteRowsWithManifestUpdate(
              (OverwriteRowsWithManifest) request.updates().get(0), baseTable);
        } else if (request.updates().get(0) instanceof OverwriteRowsWithDeleteFilter) {
          return handleOverwriteRowsWithDeleteFilterUpdate(
              (OverwriteRowsWithDeleteFilter) request.updates().get(0), baseTable);
        } else {
          TableOperations ops = baseTable.operations();
          finalMetadata = commit(ops, request);
        }
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    return LoadTableResponse.builder().withTableMetadata(finalMetadata).build();
  }

  private static LoadTableResponse handleRenameUpdate(
      Catalog catalog, TableIdentifier ident, UpdateTableRequest request, BaseTable baseTable) {
    RenameTable renameRequest = (RenameTable) request.updates().get(0);
    catalog.renameTable(ident, renameRequest.getDestination());
    return LoadTableResponse.builder().withTableMetadata(baseTable.operations().current()).build();
  }

  private static LoadTableResponse handleOverwriteRowsWithManifestUpdate(
      OverwriteRowsWithManifest update, BaseTable table) {
    List<ManifestLocation> addedManifests = update.getAddedManifestLocations();
    List<ManifestLocation> removedManifests = update.getRemovedManifestLocations();
    if (addedManifests != null && removedManifests == null) {
      appendFiles(table, addedManifests);
    } else if (addedManifests == null && removedManifests != null) {
      deleteFiles(table, removedManifests);
    } else if (removedManifests != null) {
      overwriteFiles(table, addedManifests, removedManifests);
    }
    return LoadTableResponse.builder().withTableMetadata(table.operations().current()).build();
  }

  private static void appendFiles(BaseTable table, List<ManifestLocation> manifestLocations) {
    AppendFiles append = table.newAppend();
    for (ManifestLocation manifestLocation : manifestLocations) {
      List<DataFile> dataFiles = readManifest(table, manifestLocation);
      for (DataFile dataFile : dataFiles) {
        append.appendFile(dataFile);
      }
    }
    append.commit();
  }

  private static void deleteFiles(BaseTable table, List<ManifestLocation> manifestLocations) {
    DeleteFiles delete = table.newDelete();
    for (ManifestLocation manifestLocation : manifestLocations) {
      List<DataFile> dataFiles = readManifest(table, manifestLocation);
      for (DataFile dataFile : dataFiles) {
        delete.deleteFile(dataFile);
      }
    }
    delete.commit();
  }

  private static void overwriteFiles(
      BaseTable table,
      List<ManifestLocation> addedManifests,
      List<ManifestLocation> removedManifests) {
    OverwriteFiles overwrite = table.newOverwrite();

    for (ManifestLocation manifestLocation : removedManifests) {
      List<DataFile> dataFiles = readManifest(table, manifestLocation);
      for (DataFile dataFile : dataFiles) {
        overwrite.deleteFile(dataFile);
      }
    }

    for (ManifestLocation manifestLocation : addedManifests) {
      List<DataFile> dataFiles = readManifest(table, manifestLocation);
      for (DataFile dataFile : dataFiles) {
        overwrite.addFile(dataFile);
      }
    }

    overwrite.commit();
  }

  private static LoadTableResponse handleOverwriteRowsWithDeleteFilterUpdate(
      OverwriteRowsWithDeleteFilter update, BaseTable table) {
    List<ManifestLocation> addedManifests = update.getAddedManifestLocations();
    String deleteFilter = update.getDeleteFilter();
    table.newDelete().deleteFromRowFilter(ExpressionParser.fromJson(deleteFilter)).commit();
    appendFiles(table, addedManifests);
    return LoadTableResponse.builder().withTableMetadata(table.operations().current()).build();
  }

  public static List<DataFile> readManifest(Table table, ManifestLocation location) {
    InputFile inputFile = table.io().newInputFile(location.getLocation());
    try {
      return IcebergDataManifestParser.readDataFilesFromJson(inputFile, table.specs());
    } catch (IOException e) {
      throw new RuntimeException("Failed to read data manifest file: " + location.getLocation(), e);
    }
  }

  private static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateRequirement> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private static TableMetadata create(TableOperations ops, UpdateTableRequest request) {
    // the only valid requirement is that the table will be created
    request.requirements().forEach(requirement -> requirement.validate(ops.current()));
    Optional<Integer> formatVersion =
        request.updates().stream()
            .filter(update -> update instanceof UpgradeFormatVersion)
            .map(update -> ((UpgradeFormatVersion) update).formatVersion())
            .findFirst();

    TableMetadata.Builder builder =
        formatVersion.map(TableMetadata::buildFromEmpty).orElseGet(TableMetadata::buildFromEmpty);
    request.updates().forEach(update -> update.applyTo(builder));
    // create transactions do not retry. if the table exists, retrying is not a solution
    ops.commit(null, builder.build());

    return ops.current();
  }

  static TableMetadata commit(TableOperations ops, UpdateTableRequest request) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
                isRetry.set(true);

                // validate requirements
                try {
                  request.requirements().forEach(requirement -> requirement.validate(base));
                } catch (CommitFailedException e) {
                  // wrap and rethrow outside of tasks to avoid unnecessary retry
                  throw new ValidationFailureException(e);
                }

                // apply changes
                TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(base);
                request.updates().forEach(update -> update.applyTo(metadataBuilder));

                TableMetadata updated = metadataBuilder.build();
                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed
                  return;
                }

                // commit
                taskOps.commit(base, updated);
              });

    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }
}
