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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import software.amazon.glue.RequestResponseTestBase;

public class TestLoadCatalogResponse extends RequestResponseTestBase<LoadCatalogResponse> {

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson =
        "{\"identifier\":\"id\",\"use-extensions\":false,\"target-redshift-catalog-identifier\":null}";
    LoadCatalogResponse response =
        ImmutableLoadCatalogResponse.builder()
            .identifier("id")
            .useExtensions(false)
            .targetRedshiftCatalogIdentifier(null)
            .build();
    assertRoundTripSerializesEquallyFrom(fullJson, response);
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"identifier", "use-extensions", "target-redshift-catalog-identifier"};
  }

  @Override
  public LoadCatalogResponse createExampleInstance() {
    return ImmutableLoadCatalogResponse.builder()
        .identifier("id")
        .useExtensions(true)
        .targetRedshiftCatalogIdentifier(null)
        .build();
  }

  @Override
  public void assertEquals(LoadCatalogResponse actual, LoadCatalogResponse expected) {
    assertThat(actual).isEqualTo(expected);
  }

  @Override
  public LoadCatalogResponse deserialize(String json) throws JsonProcessingException {
    return LoadCatalogResponseParser.fromJson(json);
  }
}
