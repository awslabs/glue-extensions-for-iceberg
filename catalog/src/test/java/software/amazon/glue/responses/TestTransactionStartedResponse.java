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
package software.amazon.glue.responses;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import software.amazon.glue.RequestResponseTestBase;

public class TestTransactionStartedResponse
    extends RequestResponseTestBase<TransactionStartedResponse> {

  private static final String TRANSACTION = "txn";

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson = "{\"transaction\":\"txn\"}";
    TransactionStartedResponse req =
        TransactionStartedResponse.builder().transaction(TRANSACTION).build();
    assertRoundTripSerializesEquallyFrom(fullJson, req);
  }

  @Test
  public void testDeserializeInvalidResponse() {
    String jsonIncorrectTypeForProperties = "{\"transaction\":[\"accounting\",\"tax\"]}";
    assertThatThrownBy(() -> deserialize(jsonIncorrectTypeForProperties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse to a string value: transaction");

    String jsonMisspelledKeys = "{\"transactionsssss\": \"txn\"}";
    assertThatThrownBy(() -> deserialize(jsonMisspelledKeys))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse TransactionStartedResponse: missing transaction");

    String emptyJson = "{}";
    assertThatThrownBy(() -> deserialize(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse TransactionStartedResponse: missing transaction");

    assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    assertThatThrownBy(() -> TransactionStartedResponse.builder().transaction(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid transaction: null");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"transaction"};
  }

  @Override
  public TransactionStartedResponse createExampleInstance() {
    return TransactionStartedResponse.builder().transaction(TRANSACTION).build();
  }

  @Override
  public void assertEquals(TransactionStartedResponse actual, TransactionStartedResponse expected) {
    assertThat(actual.transaction()).isEqualTo(expected.transaction());
  }

  @Override
  public TransactionStartedResponse deserialize(String json) throws JsonProcessingException {
    TransactionStartedResponse response =
        mapper().readValue(json, TransactionStartedResponse.class);
    response.validate();
    return response;
  }
}
