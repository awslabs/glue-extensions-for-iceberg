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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializationUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.FederatedDatabase;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.glue.responses.CheckTransactionStatusResponse;
import software.amazon.glue.responses.CheckTransactionStatusResponse.Status;
import software.amazon.glue.responses.ImmutableLoadCatalogResponse;
import software.amazon.glue.responses.LoadCatalogResponse;

public class TestGlueUtil {

  @Test
  public void testStripTrailingSlash() {
    String[][] testCases =
        new String[][] {
          new String[] {"https://foo", "https://foo"},
          new String[] {"https://foo/", "https://foo"},
          new String[] {"https://foo////", "https://foo"},
          new String[] {null, null}
        };

    for (String[] testCase : testCases) {
      String input = testCase[0];
      String expected = testCase[1];
      assertThat(GlueUtil.stripTrailingSlash(input)).isEqualTo(expected);
    }
  }

  @Test
  public void testRoundTripUrlEncodeDecodeNamespace() {
    // Namespace levels and their expected url encoded form
    Object[][] testCases =
        new Object[][] {
          new Object[] {new String[] {"dogs"}, "dogs"},
          new Object[] {new String[] {"dogs.named.hank"}, "dogs.named.hank"},
          new Object[] {new String[] {"dogs/named/hank"}, "dogs%2Fnamed%2Fhank"}
        };

    for (Object[] namespaceWithEncoding : testCases) {
      String[] levels = (String[]) namespaceWithEncoding[0];
      String encodedNs = (String) namespaceWithEncoding[1];

      Namespace namespace = Namespace.of(levels);

      // To be placed into a URL path as query parameter or path parameter
      assertThat(GlueUtil.encodeNamespace(namespace)).isEqualTo(encodedNs);

      // Decoded (after pulling as String) from URL
      Namespace asNamespace = GlueUtil.decodeNamespace(encodedNs);
      assertThat(asNamespace).isEqualTo(namespace);
    }
  }

  @Test
  public void testNamespaceUrlEncodeDecodeDoesNotAllowNull() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> GlueUtil.encodeNamespace(null))
        .withMessage("Invalid namespace: null");

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> GlueUtil.decodeNamespace(null))
        .withMessage("Invalid namespace: null");
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2URLEncoding() {
    // from OAuth2, RFC 6749 Appendix B.
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String expected = "+%25%26%2B%C2%A3%E2%82%AC";

    assertThat(GlueUtil.encodeString(utf8)).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2FormDataEncoding() {
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String asString = "+%25%26%2B%C2%A3%E2%82%AC";
    Map<String, String> formData = ImmutableMap.of("client_id", "12345", "client_secret", utf8);
    String expected = "client_id=12345&client_secret=" + asString;

    assertThat(GlueUtil.encodeFormData(formData)).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2FormDataDecoding() {
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String asString = "+%25%26%2B%C2%A3%E2%82%AC";
    Map<String, String> expected = ImmutableMap.of("client_id", "12345", "client_secret", utf8);
    String formString = "client_id=12345&client_secret=" + asString;

    assertThat(GlueUtil.decodeFormData(formString)).isEqualTo(expected);
  }

  @Test
  public void testPollTransactionStatusUntilFinishedSucceeds() {
    String path = "v1/transactions/status";
    String transaction = "txn";
    Map<String, String> headers = new HashMap<>();
    long sleepDurationMs = 1000;
    Supplier<Map<String, String>> headersSupplier = () -> headers;

    GlueExtensionsClient mockClient = mock(GlueExtensionsClient.class);
    CheckTransactionStatusResponse finishedResponse =
        CheckTransactionStatusResponse.builder().withStatus(Status.FINISHED).build();
    when(mockClient.post(eq(path), any(), eq(CheckTransactionStatusResponse.class), any()))
        .thenReturn(finishedResponse);
    GlueUtil.checkTransactionStatusUntilFinished(mockClient, path, transaction, sleepDurationMs);

    verify(mockClient, times(1))
        .post(eq(path), any(), eq(CheckTransactionStatusResponse.class), any());
  }

  @Test
  public void testPollTransactionStatusUntilFinishedFailedTransaction() {
    String path = "v1/transactions/status";
    String transaction = "txn";
    long sleepDurationMs = 1000;
    String errorMessage = String.format("Transaction %s canceled", transaction);
    GlueExtensionsClient mockClient = mock(GlueExtensionsClient.class);

    CheckTransactionStatusResponse failedResponse =
        CheckTransactionStatusResponse.builder()
            .withStatus(Status.FAILED)
            .withError(errorMessage)
            .build();
    when(mockClient.post(eq(path), any(), eq(CheckTransactionStatusResponse.class), any()))
        .thenReturn(failedResponse);
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                GlueUtil.checkTransactionStatusUntilFinished(
                    mockClient, path, transaction, sleepDurationMs));

    verify(mockClient, times(1))
        .post(eq(path), any(), eq(CheckTransactionStatusResponse.class), any());
    Assertions.assertThat(exception.getMessage()).contains(errorMessage);
  }

  @Test
  public void testPollTransactionStatusUntilFinishedInProgressState() {
    String path = "v1/transactions/status";
    String transaction = "txn";
    long sleepDurationMs = 100;
    GlueExtensionsClient mockClient = mock(GlueExtensionsClient.class);

    CheckTransactionStatusResponse inProgressResponse =
        CheckTransactionStatusResponse.builder().withStatus(Status.STARTED).build();
    CheckTransactionStatusResponse finishedResponse =
        CheckTransactionStatusResponse.builder().withStatus(Status.FINISHED).build();

    when(mockClient.post(eq(path), any(), eq(CheckTransactionStatusResponse.class), any()))
        .thenReturn(inProgressResponse)
        .thenReturn(finishedResponse);

    GlueUtil.checkTransactionStatusUntilFinished(mockClient, path, transaction, sleepDurationMs);
    verify(mockClient, atLeast(2))
        .post(eq(path), any(), eq(CheckTransactionStatusResponse.class), any());
  }

  @Test
  public void testPollTransactionStatusUntilFinishedNullTxnId() {
    String path = "v1/transactions/status";
    long sleepDurationMs = 100;
    GlueExtensionsClient mockClient = mock(GlueExtensionsClient.class);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GlueUtil.checkTransactionStatusUntilFinished(
                    mockClient, path, null, sleepDurationMs));
    assert exception.getMessage().equals("Invalid transaction: null");
  }

  @Test
  public void testUseExtensionsForGlueDatabase() {
    Assertions.assertThat(GlueUtil.useExtensionsForGlueDatabase(null, null)).isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueDatabase(null, Database.builder().name("db").build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueDatabase(
                null,
                Database.builder()
                    .name("db")
                    .federatedDatabase(
                        FederatedDatabase.builder()
                            .connectionName("lambda")
                            .identifier("id")
                            .build())
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueDatabase(
                null,
                Database.builder()
                    .name("db")
                    .federatedDatabase(
                        FederatedDatabase.builder()
                            .connectionName("aws:redshift")
                            .identifier("id")
                            .build())
                    .build()))
        .isTrue();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueDatabase(
                ImmutableLoadCatalogResponse.builder()
                    .identifier("arn")
                    .useExtensions(true)
                    .targetRedshiftCatalogIdentifier(null)
                    .build(),
                Database.builder()
                    .name("db")
                    .federatedDatabase(
                        FederatedDatabase.builder()
                            .connectionName("something")
                            .identifier("id")
                            .build())
                    .build()))
        .isTrue();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueDatabase(
                ImmutableLoadCatalogResponse.builder()
                    .identifier("arn")
                    .useExtensions(true)
                    .targetRedshiftCatalogIdentifier("arn:catalog")
                    .build(),
                Database.builder()
                    .name("db")
                    .federatedDatabase(
                        FederatedDatabase.builder()
                            .connectionName("something")
                            .identifier("id")
                            .build())
                    .build()))
        .isTrue();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueDatabase(
                ImmutableLoadCatalogResponse.builder()
                    .identifier("arn")
                    .useExtensions(false)
                    .build(),
                Database.builder()
                    .name("db")
                    .federatedDatabase(
                        FederatedDatabase.builder()
                            .connectionName("something")
                            .identifier("id")
                            .build())
                    .build()))
        .isFalse();
  }

  @Test
  public void testUseExtensionsForIcebergNamespace() {
    Assertions.assertThat(GlueUtil.useExtensionsForIcebergNamespace(null)).isFalse();
    Assertions.assertThat(GlueUtil.useExtensionsForIcebergNamespace(ImmutableMap.of())).isFalse();
    Assertions.assertThat(GlueUtil.useExtensionsForIcebergNamespace(ImmutableMap.of("k", "v")))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForIcebergNamespace(ImmutableMap.of("type", "lambda")))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForIcebergNamespace(ImmutableMap.of("type", "redshift")))
        .isTrue();
  }

  @Test
  public void testUseExtensionsForGlueTableScanPlanningDisabled() {
    LoadCatalogResponse catalogResponse =
        ImmutableLoadCatalogResponse.builder().identifier("id").useExtensions(true).build();
    Assertions.assertThat(GlueUtil.useExtensionsForGlueTable(null, null)).isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(catalogResponse, Table.builder().build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder().name("tbl").isRegisteredWithLakeFormation(false).build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder().name("tbl").isRegisteredWithLakeFormation(true).build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .storageDescriptor(StorageDescriptor.builder().location("location").build())
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .storageDescriptor(StorageDescriptor.builder().inputFormat("format").build())
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .storageDescriptor(
                        StorageDescriptor.builder().inputFormat("RedshiftFormat").build())
                    .build()))
        .isTrue();
  }

  @Test
  public void testUseExtensionsForGlueTableScanPlanningEnabled() {
    LoadCatalogResponse catalogResponse =
        ImmutableLoadCatalogResponse.builder().identifier("id").useExtensions(true).build();

    Assertions.assertThat(GlueUtil.useExtensionsForGlueTable(null, null)).isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(catalogResponse, Table.builder().build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder().name("tbl").isRegisteredWithLakeFormation(false).build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder().name("tbl").isRegisteredWithLakeFormation(true).build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .storageDescriptor(StorageDescriptor.builder().location("location").build())
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .storageDescriptor(StorageDescriptor.builder().inputFormat("format").build())
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .storageDescriptor(
                        StorageDescriptor.builder().inputFormat("RedshiftFormat").build())
                    .build()))
        .isTrue();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder().name("tbl").parameters(ImmutableMap.of("key", "val")).build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .parameters(
                        ImmutableMap.of("aws.server-side-capabilities.scan-planning", "false"))
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .parameters(
                        ImmutableMap.of("aws.server-side-capabilities.scan-planning", "true"))
                    .build()))
        .isTrue();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .parameters(
                        ImmutableMap.of("aws.server-side-capabilities.data-commit", "false"))
                    .build()))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForGlueTable(
                catalogResponse,
                Table.builder()
                    .name("tbl")
                    .parameters(ImmutableMap.of("aws.server-side-capabilities.data-commit", "true"))
                    .build()))
        .isTrue();
  }

  @Test
  public void testUseExtensionsForIcebergTable() {
    Assertions.assertThat(GlueUtil.useExtensionsForIcebergTable(null)).isFalse();
    Assertions.assertThat(GlueUtil.useExtensionsForIcebergTable(ImmutableMap.of())).isFalse();
    Assertions.assertThat(GlueUtil.useExtensionsForIcebergTable(ImmutableMap.of("k", "v")))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForIcebergTable(ImmutableMap.of("aws.write.format", "parquet")))
        .isFalse();
    Assertions.assertThat(
            GlueUtil.useExtensionsForIcebergTable(ImmutableMap.of("aws.write.format", "rms")))
        .isTrue();
  }

  @Test
  public void testConvertGlueCatalogIdToRestCatalogPath() {
    Assertions.assertThat(GlueUtil.convertGlueCatalogIdToCatalogPath(null)).isEqualTo(":");
    Assertions.assertThat(GlueUtil.convertGlueCatalogIdToCatalogPath("123456789012"))
        .isEqualTo("123456789012");
    Assertions.assertThat(GlueUtil.convertGlueCatalogIdToCatalogPath("123456789012:cat1"))
        .isEqualTo("123456789012:cat1");
    Assertions.assertThat(GlueUtil.convertGlueCatalogIdToCatalogPath("123456789012:cat1/cat2"))
        .isEqualTo("123456789012:cat1:cat2");
    Assertions.assertThat(GlueUtil.convertGlueCatalogIdToCatalogPath("cat1/cat2"))
        .isEqualTo("cat1:cat2");
  }

  @Test
  public void testRoundtripGlueTableFileIOSerialization() {
    GlueExtensionsClient client =
        GlueExtensionsClient.builder()
            .withProperties(GlueTestUtil.TEST_PROPERTIES)
            .withSessionProperties(GlueTestUtil.TEST_SESSION_PROPERTIES)
            .withEndpoint(GlueTestUtil.TEST_ENDPOINT)
            .build();
    FileIO io =
        GlueUtil.configureGlueTableFileIO(
            GlueTestUtil.TEST_PROPERTIES,
            client,
            "table/path",
            GlueTestUtil.glueLoadTableConfig(),
            null);

    byte[] bytes = SerializationUtil.serializeToBytes(io);
    SerializationUtil.deserializeFromBytes(bytes);
  }

  @Test
  public void testConfigureLocationProviderDefault() {
    Assertions.assertThat(
            GlueUtil.configureLocationProvider(
                GlueTestUtil.TEST_PROPERTIES,
                "tableLocation",
                "table/path",
                GlueTestUtil.glueLoadTableConfig()))
        .isInstanceOf(GlueTableLocationProvider.class);
  }

  @Test
  public void testConfigureLocationProviderWithStagingLocation() {
    GlueExtensionsProperties properties =
        new GlueExtensionsProperties(
            ImmutableMap.of(GlueExtensionsProperties.TABLE_METADATA_USE_STAGING_LOCATION, "true"));
    Assertions.assertThat(
            GlueUtil.configureLocationProvider(
                properties, "tableLocation", "table/path", GlueTestUtil.glueLoadTableConfig()))
        .satisfies(result -> result.getClass().getName().contains("ObjectStorageLocationProvider"));
  }
}
