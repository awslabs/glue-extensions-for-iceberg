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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGlueExtensionsTableOperations {

  @Test
  public void testRedshiftTableIdentifier() {
    Assertions.assertThat(
            GlueExtensionsTableOperations.redshiftTableIdentifier(
                "arn:aws:glue:us-east-1:123456789012:catalog/rs_ns/rs_db",
                false,
                "public",
                "table1"))
        .isEqualTo("\"rs_db@rs_ns\".\"public\".\"table1\"");
    Assertions.assertThat(
            GlueExtensionsTableOperations.redshiftTableIdentifier(
                "arn:aws:glue:us-east-1:123456789012:catalog/rs-ns/rs-db",
                false,
                "public",
                "table1"))
        .isEqualTo("\"rs-db@rs-ns\".\"public\".\"table1\"");
    Assertions.assertThat(
            GlueExtensionsTableOperations.redshiftTableIdentifier(
                "arn:aws:glue:us-east-1:123456789012:catalog/linkcontainer",
                true,
                "public",
                "table1"))
        .isEqualTo("\"linkcontainer@123456789012\".\"public\".\"table1\"");
  }

  @Test
  public void testParentCatalogArn() {
    Assertions.assertThat(
            GlueExtensionsTableOperations.parentCatalogArn(
                "arn:aws:glue:us-east-1:123456789012:catalog/rs_ns/rs_db/rs_schema", false))
        .isEqualTo("arn:aws:glue:us-east-1:123456789012:catalog/rs_ns/rs_db");
    Assertions.assertThat(
            GlueExtensionsTableOperations.parentCatalogArn(
                "arn:aws:glue:us-east-1:123456789012:catalog/rs_ns/rs_db", false))
        .isEqualTo("arn:aws:glue:us-east-1:123456789012:catalog/rs_ns");
    Assertions.assertThat(
            GlueExtensionsTableOperations.parentCatalogArn(
                "arn:aws:glue:us-east-1:123456789012:catalog/rs_ns", false))
        .isEqualTo("arn:aws:glue:us-east-1:123456789012:catalog");
    Assertions.assertThat(
            GlueExtensionsTableOperations.parentCatalogArn(
                "arn:aws:glue:us-east-1:123456789012:linkcontainer", true))
        .isEqualTo("arn:aws:glue:us-east-1:123456789012:linkcontainer");
  }
}
