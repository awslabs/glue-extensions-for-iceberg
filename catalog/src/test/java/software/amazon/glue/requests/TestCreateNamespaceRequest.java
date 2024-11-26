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
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import software.amazon.glue.RequestResponseTestBase;

public class TestCreateNamespaceRequest extends RequestResponseTestBase<CreateNamespaceRequest> {

  /* Values used to fill in request fields */
  private static final Namespace NAMESPACE = Namespace.of("accounting", "tax");
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("owner", "Hank");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{\"owner\":\"Hank\"}}";
    CreateNamespaceRequest req =
        CreateNamespaceRequest.builder().withNamespace(NAMESPACE).setProperties(PROPERTIES).build();
    assertRoundTripSerializesEquallyFrom(fullJson, req);

    String jsonEmptyProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{}}";
    CreateNamespaceRequest reqWithExplicitEmptyProperties =
        CreateNamespaceRequest.builder()
            .withNamespace(NAMESPACE)
            .setProperties(EMPTY_PROPERTIES)
            .build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, reqWithExplicitEmptyProperties);

    CreateNamespaceRequest reqWithImplicitEmptyProperties =
        CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, reqWithImplicitEmptyProperties);

    String jsonWithEmptyNamespace = "{\"namespace\":[],\"properties\":{}}";
    CreateNamespaceRequest reqUsingEmptyNamespace =
        CreateNamespaceRequest.builder().withNamespace(Namespace.empty()).build();
    assertRoundTripSerializesEquallyFrom(jsonWithEmptyNamespace, reqUsingEmptyNamespace);
  }

  @Test
  // Test cases that can't be constructed with our Builder class but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    CreateNamespaceRequest req = CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build();
    String jsonWithNullProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":null}";
    assertEquals(deserialize(jsonWithNullProperties), req);

    String jsonWithMissingProperties = "{\"namespace\":[\"accounting\",\"tax\"]}";
    assertEquals(deserialize(jsonWithMissingProperties), req);
  }

  @Test
  public void testDeserializeInvalidRequest() {
    String jsonIncorrectTypeForNamespace =
        "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}";
    assertThatThrownBy(() -> deserialize(jsonIncorrectTypeForNamespace))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageStartingWith("Cannot parse string array from non-array");

    String jsonIncorrectTypeForProperties =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}";
    assertThatThrownBy(() -> deserialize(jsonIncorrectTypeForProperties))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageStartingWith("Cannot deserialize value of type");

    String jsonMisspelledKeys =
        "{\"namepsace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}";
    assertThatThrownBy(() -> deserialize(jsonMisspelledKeys))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    String emptyJson = "{}";
    assertThatThrownBy(() -> deserialize(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    assertThatThrownBy(() -> CreateNamespaceRequest.builder().withNamespace(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid namespace: null");

    assertThatThrownBy(() -> CreateNamespaceRequest.builder().setProperties(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection of properties: null");

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");

    assertThatThrownBy(() -> CreateNamespaceRequest.builder().setProperties(mapWithNullKey).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid property: null");

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");

    assertThatThrownBy(
            () -> CreateNamespaceRequest.builder().setProperties(mapWithMultipleNullValues).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value for properties [a]: null");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"namespace", "properties"};
  }

  @Override
  public CreateNamespaceRequest createExampleInstance() {
    return CreateNamespaceRequest.builder()
        .withNamespace(NAMESPACE)
        .setProperties(PROPERTIES)
        .build();
  }

  @Override
  public void assertEquals(CreateNamespaceRequest actual, CreateNamespaceRequest expected) {
    assertThat(actual.namespace()).isEqualTo(expected.namespace());
    assertThat(actual.properties()).isEqualTo(expected.properties());
  }

  @Override
  public CreateNamespaceRequest deserialize(String json) throws JsonProcessingException {
    CreateNamespaceRequest request = mapper().readValue(json, CreateNamespaceRequest.class);
    request.validate();
    return request;
  }
}
