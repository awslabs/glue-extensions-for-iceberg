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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.BaseSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.glue.operations.RenameTable;
import software.amazon.glue.requests.CreateNamespaceRequest;
import software.amazon.glue.requests.CreateTableRequest;
import software.amazon.glue.requests.UpdateTableRequest;
import software.amazon.glue.responses.LoadCatalogResponse;
import software.amazon.glue.responses.LoadTableResponse;
import software.amazon.glue.responses.TransactionStartedResponse;

public class GlueCatalogSessionExtensions extends BaseSessionCatalog implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(GlueCatalogSessionExtensions.class);

  private String name;
  private Map<String, String> catalogProperties;
  private GlueExtensionsProperties extensionsProperties;
  private GlueExtensionsEndpoint endpoint;
  private GlueExtensionsPaths paths;
  private MetricsReporter reporter;
  private CloseableGroup closeables;
  private Cache<String, GlueExtensionsSessionProperties> sessionPropertiesCache;
  private Cache<String, GlueExtensionsClient> extensionsClientCache;
  private Cache<String, LoadCatalogResponse> catalogResponseCache;
  private Catalog delegate;
  private BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder;

  public GlueCatalogSessionExtensions() {}

  public GlueCatalogSessionExtensions(
      BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder) {
    this.ioBuilder = ioBuilder;
  }

  public GlueCatalogSessionExtensions(
      String name,
      Map<String, String> catalogProperties,
      GlueExtensionsProperties extensionsProperties,
      GlueExtensionsEndpoint endpoint,
      GlueExtensionsPaths paths,
      Catalog delegate) {
    this.name = name;
    this.catalogProperties = catalogProperties;
    this.reporter = BlackholeMetricsReporter.instance();
    this.extensionsProperties = extensionsProperties;
    this.endpoint = endpoint;
    this.paths = paths;
    this.delegate = delegate;
    initializeCachingResources();
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Properties must not be null");
    this.name = name;
    this.catalogProperties = properties;
    this.extensionsProperties = new GlueExtensionsProperties(properties);
    this.reporter = loadMetricsReporter(properties);
    this.endpoint = GlueExtensionsEndpoint.from(properties);
    this.paths = GlueExtensionsPaths.from(extensionsProperties.glueCatalogId());
    if (extensionsProperties.delegateCatalogImpl() != null) {
      this.delegate =
          CatalogUtil.loadCatalog(
              extensionsProperties.delegateCatalogImpl(),
              name + "_delegate",
              extensionsProperties.delegateCatalogProperties(),
              null);
    }

    initializeCachingResources();
  }

  private FileIO tableFileIO(SessionContext context, Map<String, String> config) {
    if (null != ioBuilder) {
      return ioBuilder.apply(context, config);
    }
    return null;
  }

  private void initializeCachingResources() {
    this.sessionPropertiesCache =
        Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMillis(extensionsProperties.sessionCacheExpirationMs()))
            .build();
    this.extensionsClientCache =
        Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMillis(extensionsProperties.sessionCacheExpirationMs()))
            .build();
    this.catalogResponseCache =
        Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMillis(extensionsProperties.sessionCacheExpirationMs()))
            .build();

    this.closeables = new CloseableGroup();
    this.closeables.setSuppressCloseFailure(true);
    if (delegate != null && delegate instanceof Closeable) {
      this.closeables.addCloseable((Closeable) delegate);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Map<String, String> properties() {
    return catalogProperties;
  }

  public boolean useExtensionsForGlueTable(
      SessionContext context, software.amazon.awssdk.services.glue.model.Table glueTable) {
    LoadCatalogResponse catalogResponse = null;
    try {
      catalogResponse = catalogResponse(context);
    } catch (Exception e) {
      LOG.warn("Fail to load catalog {}: {}", paths.catalog(), e.getMessage());
    }
    return GlueUtil.useExtensionsForGlueTable(catalogResponse, glueTable);
  }

  public boolean useExtensionsForGlueDatabase(SessionContext context, Database glueDatabase) {
    LoadCatalogResponse catalogResponse = null;
    try {
      catalogResponse = catalogResponse(context);
    } catch (Exception e) {
      LOG.warn("Fail to load catalog {}: {}", paths.catalog(), e.getMessage());
    }
    return GlueUtil.useExtensionsForGlueDatabase(catalogResponse, glueDatabase);
  }

  private GlueExtensionsSessionProperties sessionProperties(SessionContext context) {
    return sessionPropertiesCache.get(
        context.sessionId(), sessionId -> new GlueExtensionsSessionProperties(context));
  }

  private GlueExtensionsClient sessionExtensionsClient(SessionContext context) {
    return extensionsClientCache.get(
        context.sessionId(),
        sessionId -> {
          GlueExtensionsClient client =
              GlueExtensionsClient.builder()
                  .withEndpoint(endpoint)
                  .withProperties(extensionsProperties)
                  .withSessionProperties(sessionProperties(context))
                  .build();
          closeables.addCloseable(client);
          return client;
        });
  }

  private LoadCatalogResponse catalogResponse(SessionContext context) {
    return catalogResponseCache.get(
        context.sessionId(),
        sessionId ->
            sessionExtensionsClient(context)
                .get(
                    paths.catalog(),
                    LoadCatalogResponse.class,
                    ErrorHandlers.catalogErrorHandler()));
  }

  private MetricsReporter loadMetricsReporter(Map<String, String> properties) {
    if (properties.containsKey(CatalogProperties.METRICS_REPORTER_IMPL)) {
      return CatalogUtil.loadMetricsReporter(properties);
    }
    return BlackholeMetricsReporter.instance();
  }

  @Override
  public List<TableIdentifier> listTables(SessionContext context, Namespace ns) {
    GlueUtil.checkDelegateCatalogAvailable(delegate, "ListTables");
    return delegate.listTables(ns);
  }

  @Override
  public boolean dropTable(SessionContext context, TableIdentifier identifier) {
    checkIdentifierIsValid(identifier);
    GlueUtil.checkSingleLevelNamespace(identifier.namespace());

    try {
      TransactionStartedResponse response =
          sessionExtensionsClient(context)
              .delete(
                  paths.table(identifier),
                  TransactionStartedResponse.class,
                  ErrorHandlers.tableErrorHandler());

      GlueUtil.checkTransactionStatusUntilFinished(
          sessionExtensionsClient(context),
          paths.transactionStatus(),
          response.transaction(),
          extensionsProperties.transactionStatusCheckIntervalMs());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public boolean purgeTable(SessionContext context, TableIdentifier identifier) {
    checkIdentifierIsValid(identifier);
    GlueUtil.checkSingleLevelNamespace(identifier.namespace());

    try {
      TransactionStartedResponse response =
          sessionExtensionsClient(context)
              .delete(
                  paths.table(identifier),
                  ImmutableMap.of(
                      GlueExtensionsPaths.QUERY_PARAM_PURGE_REQUESTED,
                      GlueExtensionsPaths.PURGE_REQUESTED_DEFAULT),
                  TransactionStartedResponse.class,
                  ErrorHandlers.tableErrorHandler());

      GlueUtil.checkTransactionStatusUntilFinished(
          sessionExtensionsClient(context),
          paths.transactionStatus(),
          response.transaction(),
          extensionsProperties.transactionStatusCheckIntervalMs());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(SessionContext context, TableIdentifier from, TableIdentifier to) {
    RenameTable renameTable = new RenameTable(to);
    UpdateTableRequest request = new UpdateTableRequest(null, ImmutableList.of(renameTable));
    TransactionStartedResponse response =
        sessionExtensionsClient(context)
            .post(
                paths.table(from),
                request,
                TransactionStartedResponse.class,
                ErrorHandlers.tableErrorHandler());
    GlueUtil.checkTransactionStatusUntilFinished(
        sessionExtensionsClient(context),
        paths.transactionStatus(),
        response.transaction(),
        extensionsProperties.transactionStatusCheckIntervalMs());
  }

  private LoadTableResponse loadTableCall(SessionContext context, TableIdentifier identifier) {
    return sessionExtensionsClient(context)
        .get(paths.table(identifier), LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
  }

  @Override
  public Table loadTable(SessionContext context, TableIdentifier identifier) {
    checkIdentifierIsValid(identifier);

    MetadataTableType metadataType = MetadataTableType.from(identifier.name());
    LoadTableResponse response;
    TableIdentifier loadedIdent;

    if (metadataType == null) {

      response = loadTableCall(context, identifier);
      loadedIdent = identifier;
    } else {
      TableIdentifier baseIdent = TableIdentifier.of(identifier.namespace().levels());
      GlueUtil.checkSingleLevelNamespace(baseIdent.namespace());
      response = loadTableCall(context, baseIdent);
      loadedIdent = baseIdent;
    }

    TableIdentifier finalIdentifier = loadedIdent;
    TableMetadata tableMetadata = response.tableMetadata();
    GlueLoadTableConfig loadTableConfig = new GlueLoadTableConfig(response.config());
    GlueExtensionsTableOperations ops =
        new GlueExtensionsTableOperations(
            finalIdentifier,
            sessionExtensionsClient(context),
            paths,
            extensionsProperties,
            sessionProperties(context),
            catalogResponse(context),
            loadTableConfig,
            tableMetadata,
            tableFileIO(context, catalogProperties));
    GlueTable table = new GlueTable(ops, fullTableName(finalIdentifier), reporter);

    if (metadataType != null) {
      return MetadataTableUtils.createMetadataTableInstance(table, metadataType);
    }

    return table;
  }

  @Override
  public Catalog.TableBuilder buildTable(
      SessionContext context, TableIdentifier identifier, Schema schema) {
    return new Builder(identifier, schema, context);
  }

  @Override
  public void invalidateTable(SessionContext context, TableIdentifier ident) {}

  @Override
  public Table registerTable(
      SessionContext context, TableIdentifier ident, String metadataFileLocation) {
    GlueUtil.checkDelegateCatalogAvailable(delegate, "RegisterTable");
    return delegate.registerTable(ident, metadataFileLocation);
  }

  @Override
  public void createNamespace(
      SessionContext context, Namespace namespace, Map<String, String> metadata) {
    GlueUtil.checkSingleLevelNamespace(namespace);

    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).setProperties(metadata).build();

    // for now, ignore the response because there is no way to return it
    TransactionStartedResponse response =
        sessionExtensionsClient(context)
            .post(
                paths.namespaces(),
                request,
                TransactionStartedResponse.class,
                ErrorHandlers.namespaceErrorHandler());

    GlueUtil.checkTransactionStatusUntilFinished(
        sessionExtensionsClient(context),
        paths.transactionStatus(),
        response.transaction(),
        extensionsProperties.transactionStatusCheckIntervalMs());
  }

  @Override
  public List<Namespace> listNamespaces(SessionContext context) {
    GlueUtil.checkDelegateCatalogAvailable(delegate, "RegisterTable");
    return ((SupportsNamespaces) delegate).listNamespaces();
  }

  @Override
  public List<Namespace> listNamespaces(SessionContext context, Namespace namespace) {
    GlueUtil.checkDelegateCatalogAvailable(delegate, "RegisterTable");
    return ((SupportsNamespaces) delegate).listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(SessionContext context, Namespace ns) {
    GlueUtil.checkDelegateCatalogAvailable(delegate, "RegisterTable");
    return ((SupportsNamespaces) delegate).loadNamespaceMetadata(ns);
  }

  @Override
  public boolean dropNamespace(SessionContext context, Namespace ns) {
    checkNamespaceIsValid(ns);

    try {
      TransactionStartedResponse response =
          sessionExtensionsClient(context)
              .delete(
                  paths.namespace(ns),
                  TransactionStartedResponse.class,
                  ErrorHandlers.namespaceErrorHandler());

      GlueUtil.checkTransactionStatusUntilFinished(
          sessionExtensionsClient(context),
          paths.transactionStatus(),
          response.transaction(),
          extensionsProperties.transactionStatusCheckIntervalMs());
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean updateNamespaceMetadata(
      SessionContext context, Namespace ns, Map<String, String> updates, Set<String> removals) {
    GlueUtil.checkDelegateCatalogAvailable(delegate, "RegisterTable");
    boolean updateSucceeded = true;
    if (updates != null) {
      updateSucceeded = ((SupportsNamespaces) delegate).setProperties(ns, updates);
    }

    boolean removeSucceeded = true;
    if (removals != null) {
      removeSucceeded = ((SupportsNamespaces) delegate).removeProperties(ns, removals);
    }

    return updateSucceeded && removeSucceeded;
  }

  @Override
  public void close() throws IOException {
    if (closeables != null) {
      closeables.close();
    }
  }

  private class Builder implements Catalog.TableBuilder {

    private final TableIdentifier ident;
    private final SessionContext context;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private Schema schema;
    private PartitionSpec spec = null;
    private SortOrder writeOrder = null;
    private String location = null;
    private Catalog.TableBuilder delegateBuilder = null;

    private Builder(TableIdentifier ident, Schema schema, SessionContext context) {
      checkIdentifierIsValid(ident);
      GlueUtil.checkSingleLevelNamespace(ident.namespace());

      this.ident = ident;
      this.schema = schema;
      this.context = context;
      if (delegate != null) {
        this.delegateBuilder = delegate.buildTable(ident, schema);
      }
    }

    @Override
    public Builder withPartitionSpec(PartitionSpec tableSpec) {
      this.spec = tableSpec;
      if (delegateBuilder != null) {
        this.delegateBuilder.withPartitionSpec(tableSpec);
      }
      return this;
    }

    @Override
    public Builder withSortOrder(SortOrder tableWriteOrder) {
      this.writeOrder = tableWriteOrder;
      if (delegateBuilder != null) {
        this.delegateBuilder.withSortOrder(tableWriteOrder);
      }
      return this;
    }

    @Override
    public Builder withLocation(String tableLocation) {
      this.location = tableLocation;
      if (delegateBuilder != null) {
        this.delegateBuilder.withLocation(tableLocation);
      }
      return this;
    }

    @Override
    public Builder withProperties(Map<String, String> props) {
      if (props != null) {
        props.forEach(this::withProperty);
      }
      if (delegateBuilder != null) {
        this.delegateBuilder.withProperties(props);
      }
      return this;
    }

    @Override
    public Builder withProperty(String key, String value) {
      if (GlueTableProperties.AWS_WRITE_SORT_KEYS.equals(key)) {
        SortOrder.Builder sortOrderBuilder = SortOrder.builderFor(schema);
        String[] sortkeys = value.split(",");
        Preconditions.checkArgument(
            sortkeys.length > 0, "Cannot parse sort keys from input: %s", value);

        for (String sortkey : sortkeys) {
          sortOrderBuilder.sortBy(sortkey, SortDirection.ASC, NullOrder.NULLS_LAST);
        }
        return withSortOrder(sortOrderBuilder.build());
      }

      if (GlueTableProperties.AWS_WRITE_PRIMARY_KEYS.equals(key)) {
        String[] primaryKeys = value.split(",");
        Preconditions.checkArgument(
            primaryKeys.length > 0, "Cannot parse primary keys from input: %s", value);
        Set<Integer> primaryKeyValues =
            Arrays.stream(primaryKeys)
                .map(schema::findField)
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toSet());

        this.schema =
            new Schema(schema.schemaId(), schema.columns(), schema.getAliases(), primaryKeyValues);
        return this;
      }

      this.propertiesBuilder.put(key, value);
      if (delegateBuilder != null) {
        delegateBuilder.withProperty(key, value);
      }
      return this;
    }

    @Override
    public Table create() {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(ident.name())
              .withSchema(schema)
              .withPartitionSpec(spec)
              .withWriteOrder(writeOrder)
              .withLocation(location)
              .setProperties(propertiesBuilder.build())
              .build();

      TransactionStartedResponse transactionResponse =
          sessionExtensionsClient(context)
              .post(
                  paths.tables(ident.namespace()),
                  request,
                  TransactionStartedResponse.class,
                  ErrorHandlers.tableErrorHandler());

      GlueUtil.checkTransactionStatusUntilFinished(
          sessionExtensionsClient(context),
          paths.transactionStatus(),
          transactionResponse.transaction(),
          extensionsProperties.transactionStatusCheckIntervalMs());

      LoadTableResponse response = loadTableCall(context, ident);
      GlueLoadTableConfig loadTableConfig = new GlueLoadTableConfig(response.config());
      GlueExtensionsTableOperations ops =
          new GlueExtensionsTableOperations(
              ident,
              sessionExtensionsClient(context),
              paths,
              extensionsProperties,
              sessionProperties(context),
              catalogResponse(context),
              loadTableConfig,
              response.tableMetadata(),
              tableFileIO(context, catalogProperties));

      return new GlueTable(ops, fullTableName(ident), reporter);
    }

    @Override
    public Transaction createTransaction() {
      LoadTableResponse response = stageCreate();
      String fullName = fullTableName(ident);

      TableMetadata meta = response.tableMetadata();
      GlueLoadTableConfig loadTableConfig = new GlueLoadTableConfig(response.config());
      GlueExtensionsTableOperations ops =
          new GlueExtensionsTableOperations(
              ident,
              sessionExtensionsClient(context),
              paths,
              extensionsProperties,
              sessionProperties(context),
              catalogResponse(context),
              loadTableConfig,
              meta,
              tableFileIO(context, catalogProperties),
              GlueExtensionsTableOperations.UpdateType.CREATE,
              createChanges(meta));

      return Transactions.createTableTransaction(fullName, ops, meta, reporter);
    }

    @Override
    public Transaction replaceTransaction() {
      GlueUtil.checkDelegateCatalogAvailable(delegate, "ReplaceTransaction");
      return delegateBuilder.replaceTransaction();
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // return a create or a replace transaction, depending on whether the table exists
      // deciding whether to create or replace can't be determined on the service because schema
      // field IDs are assigned
      // at this point and then used in data and metadata files. because create and replace will
      // assign different
      // field IDs, they must be determined before any writes occur
      try {
        return replaceTransaction();
      } catch (NoSuchTableException e) {
        return createTransaction();
      }
    }

    private LoadTableResponse stageCreate() {
      Map<String, String> tableProperties = propertiesBuilder.build();

      CreateTableRequest request =
          CreateTableRequest.builder()
              .stageCreate()
              .withName(ident.name())
              .withSchema(schema)
              .withPartitionSpec(spec)
              .withWriteOrder(writeOrder)
              .withLocation(location)
              .setProperties(tableProperties)
              .build();

      TransactionStartedResponse response =
          sessionExtensionsClient(context)
              .post(
                  paths.tables(ident.namespace()),
                  request,
                  TransactionStartedResponse.class,
                  ErrorHandlers.tableErrorHandler());

      GlueUtil.checkTransactionStatusUntilFinished(
          sessionExtensionsClient(context),
          paths.transactionStatus(),
          response.transaction(),
          extensionsProperties.transactionStatusCheckIntervalMs());

      TableMetadata metadata =
          TableMetadata.newTableMetadata(
              schema,
              spec != null ? spec : PartitionSpec.unpartitioned(),
              writeOrder != null ? writeOrder : SortOrder.unsorted(),
              location != null ? location : defaultNamespaceLocation(ident.namespace()),
              tableProperties);

      return LoadTableResponse.builder().withTableMetadata(metadata).build();
    }
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    String warehouseLocation =
        PropertyUtil.propertyAsString(properties(), CatalogProperties.WAREHOUSE_LOCATION, "");
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return GlueUtil.SLASH.join(warehouseLocation, GlueUtil.SLASH.join(namespace.levels()));
    }
  }

  private static List<MetadataUpdate> createChanges(TableMetadata meta) {
    ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

    changes.add(new MetadataUpdate.AssignUUID(meta.uuid()));
    changes.add(new MetadataUpdate.UpgradeFormatVersion(meta.formatVersion()));

    Schema schema = meta.schema();
    changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
    changes.add(new MetadataUpdate.SetCurrentSchema(-1));

    PartitionSpec spec = meta.spec();
    if (spec != null && spec.isPartitioned()) {
      changes.add(new MetadataUpdate.AddPartitionSpec(spec));
    } else {
      changes.add(new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned()));
    }
    changes.add(new MetadataUpdate.SetDefaultPartitionSpec(-1));

    SortOrder order = meta.sortOrder();
    if (order != null && order.isSorted()) {
      changes.add(new MetadataUpdate.AddSortOrder(order));
    } else {
      changes.add(new MetadataUpdate.AddSortOrder(SortOrder.unsorted()));
    }
    changes.add(new MetadataUpdate.SetDefaultSortOrder(-1));

    String location = meta.location();
    if (location != null) {
      changes.add(new MetadataUpdate.SetLocation(location));
    }

    Map<String, String> properties = meta.properties();
    if (properties != null && !properties.isEmpty()) {
      changes.add(new MetadataUpdate.SetProperties(properties));
    }

    return changes.build();
  }

  private String fullTableName(TableIdentifier ident) {
    return String.format("%s.%s", name(), ident);
  }

  private void checkIdentifierIsValid(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      throw new NoSuchTableException("Invalid table identifier: %s", tableIdentifier);
    }
  }

  private void checkNamespaceIsValid(Namespace namespace) {
    if (namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Invalid namespace: %s", namespace);
    }
    GlueUtil.checkSingleLevelNamespace(namespace);
  }
}
