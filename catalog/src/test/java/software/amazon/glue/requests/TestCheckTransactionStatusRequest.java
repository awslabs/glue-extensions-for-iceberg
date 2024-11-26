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
package software.amazon.glue.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import software.amazon.glue.RequestResponseTestBase;

public class TestCheckTransactionStatusRequest
    extends RequestResponseTestBase<CheckTransactionStatusRequest> {

  private static final String TRANSACTION = "txn";

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson = "{\"transaction\":\"txn\"}";
    CheckTransactionStatusRequest req =
        CheckTransactionStatusRequest.builder().withTransaction(TRANSACTION).build();
    assertRoundTripSerializesEquallyFrom(fullJson, req);
  }

  @Test
  public void testDeserializeInvalidRequest() {
    String jsonIncorrectTypeForProperties = "{\"transaction\":[\"accounting\",\"tax\"]}";
    assertThatThrownBy(() -> deserialize(jsonIncorrectTypeForProperties))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageStartingWith("Cannot deserialize value of type");

    String jsonMisspelledKeys = "{\"transactionsssss\": \"txn\"}";
    assertThatThrownBy(() -> deserialize(jsonMisspelledKeys))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid transaction: null");

    String emptyJson = "{}";
    assertThatThrownBy(() -> deserialize(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid transaction: null");

    assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    assertThatThrownBy(() -> CheckTransactionStatusRequest.builder().withTransaction(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid transaction: null");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"transaction"};
  }

  @Override
  public CheckTransactionStatusRequest createExampleInstance() {
    return CheckTransactionStatusRequest.builder().withTransaction(TRANSACTION).build();
  }

  @Override
  public void assertEquals(
      CheckTransactionStatusRequest actual, CheckTransactionStatusRequest expected) {
    assertThat(actual.transaction()).isEqualTo(expected.transaction());
  }

  @Override
  public CheckTransactionStatusRequest deserialize(String json) throws JsonProcessingException {
    CheckTransactionStatusRequest request =
        mapper().readValue(json, CheckTransactionStatusRequest.class);
    request.validate();
    return request;
  }
}
