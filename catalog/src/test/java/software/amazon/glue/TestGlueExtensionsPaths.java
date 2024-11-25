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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGlueExtensionsPaths {
  private final GlueExtensionsPaths withCustomCatalog = GlueExtensionsPaths.from("cat1/cat2");
  private final GlueExtensionsPaths withDefaultAccountCatalog = GlueExtensionsPaths.from(null);
  private final GlueExtensionsPaths withCustomAccountCatalog =
      GlueExtensionsPaths.from("123456789012");
  private final GlueExtensionsPaths withCustomAccountCustomCatalog =
      GlueExtensionsPaths.from("123456789012:cat1/cat2");

  @Test
  public void testNamespaces() {
    assertThat(withCustomCatalog.namespaces()).isEqualTo("v1/catalogs/cat1:cat2/namespaces");
    assertThat(withDefaultAccountCatalog.namespaces()).isEqualTo("v1/catalogs/:/namespaces");
    assertThat(withCustomAccountCatalog.namespaces())
        .isEqualTo("v1/catalogs/123456789012/namespaces");
    assertThat(withCustomAccountCustomCatalog.namespaces())
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces");
  }

  @Test
  public void testNamespace() {
    Namespace ns = Namespace.of("ns");
    assertThat(withCustomCatalog.namespace(ns)).isEqualTo("v1/catalogs/cat1:cat2/namespaces/ns");
    assertThat(withDefaultAccountCatalog.namespace(ns)).isEqualTo("v1/catalogs/:/namespaces/ns");
    assertThat(withCustomAccountCatalog.namespace(ns))
        .isEqualTo("v1/catalogs/123456789012/namespaces/ns");
    assertThat(withCustomAccountCustomCatalog.namespace(ns))
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces/ns");
  }

  @Test
  public void testNamespaceWithSlash() {
    Namespace ns = Namespace.of("n/s");
    assertThat(withCustomCatalog.namespace(ns)).isEqualTo("v1/catalogs/cat1:cat2/namespaces/n%2Fs");
    assertThat(withDefaultAccountCatalog.namespace(ns)).isEqualTo("v1/catalogs/:/namespaces/n%2Fs");
    assertThat(withCustomAccountCatalog.namespace(ns))
        .isEqualTo("v1/catalogs/123456789012/namespaces/n%2Fs");
    assertThat(withCustomAccountCustomCatalog.namespace(ns))
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces/n%2Fs");
  }

  @Test
  public void testNamespaceWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assertions.assertThatThrownBy(() -> withCustomCatalog.namespace(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withDefaultAccountCatalog.namespace(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withCustomAccountCatalog.namespace(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withCustomAccountCustomCatalog.namespace(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
  }

  @Test
  public void testTables() {
    Namespace ns = Namespace.of("ns");
    assertThat(withCustomCatalog.tables(ns))
        .isEqualTo("v1/catalogs/cat1:cat2/namespaces/ns/tables");
    assertThat(withDefaultAccountCatalog.tables(ns))
        .isEqualTo("v1/catalogs/:/namespaces/ns/tables");
    assertThat(withCustomAccountCatalog.tables(ns))
        .isEqualTo("v1/catalogs/123456789012/namespaces/ns/tables");
    assertThat(withCustomAccountCustomCatalog.tables(ns))
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces/ns/tables");
  }

  @Test
  public void testTablesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    assertThat(withCustomCatalog.tables(ns))
        .isEqualTo("v1/catalogs/cat1:cat2/namespaces/n%2Fs/tables");
    assertThat(withDefaultAccountCatalog.tables(ns))
        .isEqualTo("v1/catalogs/:/namespaces/n%2Fs/tables");
    assertThat(withCustomAccountCatalog.tables(ns))
        .isEqualTo("v1/catalogs/123456789012/namespaces/n%2Fs/tables");
    assertThat(withCustomAccountCustomCatalog.tables(ns))
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces/n%2Fs/tables");
  }

  @Test
  public void testTablesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assertions.assertThatThrownBy(() -> withCustomCatalog.tables(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withDefaultAccountCatalog.tables(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withCustomAccountCatalog.tables(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withCustomAccountCustomCatalog.tables(ns))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
  }

  @Test
  public void testTable() {
    TableIdentifier ident = TableIdentifier.of("ns", "table");
    assertThat(withCustomCatalog.table(ident))
        .isEqualTo("v1/catalogs/cat1:cat2/namespaces/ns/tables/table");
    assertThat(withDefaultAccountCatalog.table(ident))
        .isEqualTo("v1/catalogs/:/namespaces/ns/tables/table");
    assertThat(withCustomAccountCatalog.table(ident))
        .isEqualTo("v1/catalogs/123456789012/namespaces/ns/tables/table");
    assertThat(withCustomAccountCustomCatalog.table(ident))
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces/ns/tables/table");
  }

  @Test
  public void testTableWithSlash() {
    TableIdentifier ident = TableIdentifier.of("n/s", "tab/le");
    assertThat(withCustomCatalog.table(ident))
        .isEqualTo("v1/catalogs/cat1:cat2/namespaces/n%2Fs/tables/tab%2Fle");
    assertThat(withDefaultAccountCatalog.table(ident))
        .isEqualTo("v1/catalogs/:/namespaces/n%2Fs/tables/tab%2Fle");
    assertThat(withCustomAccountCatalog.table(ident))
        .isEqualTo("v1/catalogs/123456789012/namespaces/n%2Fs/tables/tab%2Fle");
    assertThat(withCustomAccountCustomCatalog.table(ident))
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/namespaces/n%2Fs/tables/tab%2Fle");
  }

  @Test
  public void testTableWithMultipartNamespace() {
    TableIdentifier ident = TableIdentifier.of("n", "s", "table");
    Assertions.assertThatThrownBy(() -> withCustomCatalog.table(ident))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withDefaultAccountCatalog.table(ident))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withCustomAccountCatalog.table(ident))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
    Assertions.assertThatThrownBy(() -> withCustomAccountCustomCatalog.table(ident))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Multi-level namespace is not allowed: n.s");
  }

  @Test
  public void testTransactionStatus() {
    assertThat(withCustomCatalog.transactionStatus())
        .isEqualTo("v1/catalogs/cat1:cat2/transactions/status");
    assertThat(withDefaultAccountCatalog.transactionStatus())
        .isEqualTo("v1/catalogs/:/transactions/status");
    assertThat(withCustomAccountCatalog.transactionStatus())
        .isEqualTo("v1/catalogs/123456789012/transactions/status");
    assertThat(withCustomAccountCustomCatalog.transactionStatus())
        .isEqualTo("v1/catalogs/123456789012:cat1:cat2/transactions/status");
  }
}
