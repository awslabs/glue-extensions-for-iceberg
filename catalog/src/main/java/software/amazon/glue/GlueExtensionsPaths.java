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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;

public class GlueExtensionsPaths implements Serializable {

  // Note: Any new key should be added to GlueExtensionsProperties.CATALOG_PROPERTY_KEYS

  public static final String QUERY_PARAM_PAGE_TOKEN = "pageToken";
  public static final String QUERY_PARAM_PURGE_REQUESTED = "purgeRequested";
  public static final String PURGE_REQUESTED_DEFAULT = "true";

  private static final Joiner SLASH = Joiner.on("/").skipNulls();
  /** Indicates Glue specific APIs. */
  private static final String TRANSACTIONS_STATUS = "transactions/status";

  private static final String PATH_PARAM_CATALOGS = "catalogs";
  private static final String PATH_PARAM_NAMESPACES = "namespaces";
  private static final String PATH_PARAM_TABLES = "tables";
  private static final String PATH_V1 = "v1";
  private static final String PATH_ACTION_PLAN = "plan";
  private static final String PATH_ACTION_PREPLAN = "preplan";

  public static GlueExtensionsPaths from(String glueCatalogId) {
    String catalogPath = GlueUtil.convertGlueCatalogIdToCatalogPath(glueCatalogId);
    return new GlueExtensionsPaths(catalogPath);
  }

  private final String catalogPath;

  public GlueExtensionsPaths(String catalogPath) {
    this.catalogPath = catalogPath;
  }

  public String catalog() {
    return SLASH.join(PATH_V1, PATH_PARAM_CATALOGS, catalogPath);
  }

  public String namespaces() {
    return SLASH.join(PATH_V1, PATH_PARAM_CATALOGS, catalogPath, PATH_PARAM_NAMESPACES);
  }

  public String namespace(Namespace ns) {
    return SLASH.join(
        PATH_V1,
        PATH_PARAM_CATALOGS,
        catalogPath,
        PATH_PARAM_NAMESPACES,
        GlueUtil.encodeNamespace(ns));
  }

  public String tables(Namespace ns) {
    return SLASH.join(
        PATH_V1,
        PATH_PARAM_CATALOGS,
        catalogPath,
        PATH_PARAM_NAMESPACES,
        GlueUtil.encodeNamespace(ns),
        PATH_PARAM_TABLES);
  }

  public String table(TableIdentifier ident) {
    return SLASH.join(
        PATH_V1,
        PATH_PARAM_CATALOGS,
        catalogPath,
        PATH_PARAM_NAMESPACES,
        GlueUtil.encodeNamespace(ident.namespace()),
        PATH_PARAM_TABLES,
        GlueUtil.encodeString(ident.name()));
  }

  public String preplan(TableIdentifier identifier) {
    return SLASH.join(
        PATH_V1,
        PATH_PARAM_CATALOGS,
        catalogPath,
        PATH_PARAM_NAMESPACES,
        GlueUtil.encodeNamespace(identifier.namespace()),
        PATH_PARAM_TABLES,
        GlueUtil.encodeString(identifier.name()),
        PATH_ACTION_PREPLAN);
  }

  public String transactionStatus() {
    return SLASH.join(PATH_V1, PATH_PARAM_CATALOGS, catalogPath, TRANSACTIONS_STATUS);
  }

  public String plan(TableIdentifier identifier) {
    return SLASH.join(
        PATH_V1,
        PATH_PARAM_CATALOGS,
        catalogPath,
        PATH_PARAM_NAMESPACES,
        GlueUtil.encodeNamespace(identifier.namespace()),
        PATH_PARAM_TABLES,
        GlueUtil.encodeString(identifier.name()),
        PATH_ACTION_PLAN);
  }
}
