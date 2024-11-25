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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.services.glue.model.Database;

public class GlueCatalogExtensions implements Catalog, SupportsNamespaces, Closeable {
  private static final SessionCatalog.SessionContext EMPTY_CONTEXT =
      SessionCatalog.SessionContext.createEmpty();
  private GlueCatalogSessionExtensions sessionCatalog;

  public GlueCatalogExtensions() {}

  public GlueCatalogExtensions(GlueCatalogSessionExtensions sessionCatalog) {
    this.sessionCatalog = sessionCatalog;
  }

  public SupportsNamespaces namespaces() {
    return this;
  }

  public boolean useExtensionsForGlueTable(
      software.amazon.awssdk.services.glue.model.Table glueTable) {
    return sessionCatalog.useExtensionsForGlueTable(EMPTY_CONTEXT, glueTable);
  }

  public boolean useExtensionsForGlueDatabase(Database database) {
    return sessionCatalog.useExtensionsForGlueDatabase(EMPTY_CONTEXT, database);
  }

  @Override
  public void initialize(String name, Map<String, String> propertiesInput) {
    Preconditions.checkArgument(propertiesInput != null, "Invalid configuration: null");
    this.sessionCatalog = new GlueCatalogSessionExtensions();
    sessionCatalog.initialize(name, propertiesInput);
  }

  @Override
  public String name() {
    return sessionCatalog.name();
  }

  public Map<String, String> properties() {
    return sessionCatalog.properties();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace ns) {
    return sessionCatalog.listTables(EMPTY_CONTEXT, ns);
  }

  @Override
  public boolean tableExists(TableIdentifier ident) {
    return sessionCatalog.tableExists(EMPTY_CONTEXT, ident);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    return sessionCatalog.loadTable(EMPTY_CONTEXT, ident);
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    sessionCatalog.invalidateTable(EMPTY_CONTEXT, ident);
  }

  @Override
  public TableBuilder buildTable(TableIdentifier ident, Schema schema) {
    return sessionCatalog.buildTable(EMPTY_CONTEXT, ident, schema);
  }

  @Override
  public Table createTable(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props) {
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(props)
        .create();
  }

  @Override
  public Table createTable(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> props) {
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .withProperties(props)
        .create();
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec) {
    return sessionCatalog.buildTable(EMPTY_CONTEXT, ident, schema).withPartitionSpec(spec).create();
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return sessionCatalog.buildTable(EMPTY_CONTEXT, identifier, schema).create();
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props) {
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(props)
        .createTransaction();
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> props) {
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .withProperties(props)
        .createTransaction();
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec) {
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .createTransaction();
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
    return sessionCatalog.buildTable(EMPTY_CONTEXT, identifier, schema).createTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props,
      boolean orCreate) {
    if (orCreate) {
      return sessionCatalog
          .buildTable(EMPTY_CONTEXT, ident, schema)
          .withPartitionSpec(spec)
          .withLocation(location)
          .withProperties(props)
          .createOrReplaceTransaction();
    }
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(props)
        .replaceTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> props,
      boolean orCreate) {
    if (orCreate) {
      return sessionCatalog
          .buildTable(EMPTY_CONTEXT, ident, schema)
          .withPartitionSpec(spec)
          .withProperties(props)
          .createOrReplaceTransaction();
    }
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .withProperties(props)
        .replaceTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec, boolean orCreate) {
    if (orCreate) {
      return sessionCatalog
          .buildTable(EMPTY_CONTEXT, ident, schema)
          .withPartitionSpec(spec)
          .createOrReplaceTransaction();
    }
    return sessionCatalog
        .buildTable(EMPTY_CONTEXT, ident, schema)
        .withPartitionSpec(spec)
        .replaceTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident, Schema schema, boolean orCreate) {
    if (orCreate) {
      return sessionCatalog.buildTable(EMPTY_CONTEXT, ident, schema).createOrReplaceTransaction();
    }
    return sessionCatalog.buildTable(EMPTY_CONTEXT, ident, schema).replaceTransaction();
  }

  @Override
  public boolean dropTable(TableIdentifier ident) {
    return sessionCatalog.dropTable(EMPTY_CONTEXT, ident);
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    if (purge) {
      return sessionCatalog.purgeTable(EMPTY_CONTEXT, ident);
    }
    return sessionCatalog.dropTable(EMPTY_CONTEXT, ident);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    sessionCatalog.renameTable(EMPTY_CONTEXT, from, to);
  }

  @Override
  public Table registerTable(TableIdentifier ident, String metadataFileLocation) {
    return sessionCatalog.registerTable(EMPTY_CONTEXT, ident, metadataFileLocation);
  }

  @Override
  public void createNamespace(Namespace ns, Map<String, String> props) {
    sessionCatalog.createNamespace(EMPTY_CONTEXT, ns, props);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return sessionCatalog.listNamespaces(EMPTY_CONTEXT);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace ns) throws NoSuchNamespaceException {
    return sessionCatalog.listNamespaces(EMPTY_CONTEXT, ns);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace ns) throws NoSuchNamespaceException {
    return sessionCatalog.loadNamespaceMetadata(EMPTY_CONTEXT, ns);
  }

  @Override
  public boolean dropNamespace(Namespace ns) throws NamespaceNotEmptyException {
    return sessionCatalog.dropNamespace(EMPTY_CONTEXT, ns);
  }

  @Override
  public boolean setProperties(Namespace ns, Map<String, String> props)
      throws NoSuchNamespaceException {
    return sessionCatalog.updateNamespaceMetadata(EMPTY_CONTEXT, ns, props, null);
  }

  @Override
  public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
    return sessionCatalog.updateNamespaceMetadata(EMPTY_CONTEXT, ns, null, props);
  }

  @Override
  public void close() throws IOException {
    sessionCatalog.close();
  }
}
