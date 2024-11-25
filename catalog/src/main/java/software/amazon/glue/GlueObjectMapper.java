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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

public class GlueObjectMapper {
  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static volatile boolean isInitialized = false;

  private GlueObjectMapper() {}

  public static ObjectMapper mapper() {
    if (!isInitialized) {
      synchronized (GlueObjectMapper.class) {
        if (!isInitialized) {
          MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
          MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          MAPPER.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
          GlueSerializers.registerAll(MAPPER);
          isInitialized = true;
        }
      }
    }

    return MAPPER;
  }
}
