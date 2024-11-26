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
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Test;
import software.amazon.glue.RequestResponseTestBase;
import software.amazon.glue.responses.CheckTransactionStatusResponse.Status;

public class TestCheckTransactionStatusResponse
    extends RequestResponseTestBase<CheckTransactionStatusResponse> {

  private static final Status FINISHED_STATUS = Status.FINISHED;

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson = "{\"status\":\"FAILED\",\"error\":\"transaction failed\"}";
    CheckTransactionStatusResponse req =
        CheckTransactionStatusResponse.builder()
            .withStatus(Status.FAILED)
            .withError("transaction failed")
            .build();
    assertRoundTripSerializesEquallyFrom(fullJson, req);

    String jsonNullErrorField = "{\"status\":\"FINISHED\",\"error\":null}";
    CheckTransactionStatusResponse responseWithExplicitEmptyError =
        CheckTransactionStatusResponse.builder().withStatus(FINISHED_STATUS).build();
    assertRoundTripSerializesEquallyFrom(jsonNullErrorField, responseWithExplicitEmptyError);

    CheckTransactionStatusResponse noError =
        CheckTransactionStatusResponse.builder().withStatus(FINISHED_STATUS).build();
    String jsonWithMissingProperties = "{\"status\":\"FINISHED\"}";
    assertEquals(deserialize(jsonWithMissingProperties), noError);
  }

  @Test
  public void testDeserializeInvalidResponse() {
    String jsonResponsePropertiesHasWrongType = "{\"status\":[\"accounting\",\"tax\"]}";
    assertThatThrownBy(() -> deserialize(jsonResponsePropertiesHasWrongType))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining(
            "Cannot deserialize value of type `software.amazon.glue.responses.CheckTransactionStatusResponse$Status` from Array value");

    String jsonUnknownStatus = "{\"status\": \"UNKNOWN\"}";
    assertThatThrownBy(() -> deserialize(jsonUnknownStatus))
        .isInstanceOf(InvalidFormatException.class)
        .hasMessageContaining(
            "Cannot deserialize value of type `software.amazon.glue.responses.CheckTransactionStatusResponse$Status` from String");

    assertThatThrownBy(() -> deserialize("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status: null");

    String jsonMisspelledKeys = "{\"statussssss\": \"FINISHED\"}";
    assertThatThrownBy(() -> deserialize(jsonMisspelledKeys))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status: null");

    assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    assertThatThrownBy(() -> CheckTransactionStatusResponse.builder().withStatus(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status: null");
  }

  @Test
  public void testInvalidStatusEnum() {
    assertThatThrownBy(() -> Status.valueOf("UNKNOWN"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "No enum constant software.amazon.glue.responses.CheckTransactionStatusResponse.Status.UNKNOWN");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"status", "error"};
  }

  @Override
  public CheckTransactionStatusResponse createExampleInstance() {
    return CheckTransactionStatusResponse.builder().withStatus(FINISHED_STATUS).build();
  }

  @Override
  public void assertEquals(
      CheckTransactionStatusResponse actual, CheckTransactionStatusResponse expected) {
    assertThat(actual.status()).isEqualTo(expected.status());
    assertThat(actual.error()).isEqualTo(expected.error());
  }

  @Override
  public CheckTransactionStatusResponse deserialize(String json) throws JsonProcessingException {
    CheckTransactionStatusResponse response =
        mapper().readValue(json, CheckTransactionStatusResponse.class);
    response.validate();
    return response;
  }
}
