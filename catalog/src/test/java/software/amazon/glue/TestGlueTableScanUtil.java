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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.iceberg.GlueTableScanUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestGlueTableScanUtil {

  private static final Schema SCHEMA =
      new Schema(
          optional(3, "id", Types.IntegerType.get()), optional(4, "data", Types.StringType.get()));

  private static Stream<Arguments> expressionProvider() {
    return Stream.of(
        // basic expressions
        Arguments.of(Expressions.alwaysTrue(), Expressions.greaterThanOrEqual("id", 18), true),
        Arguments.of(Expressions.alwaysFalse(), Expressions.equal("id", 10), false),
        Arguments.of(Expressions.equal("id", 5), Expressions.equal("data", "test"), false),
        Arguments.of(Expressions.equal("id", 5), Expressions.equal("id", 5), true),
        Arguments.of(Expressions.isNull("id"), Expressions.isNull("id"), true),
        Arguments.of(Expressions.notNull("id"), Expressions.notNull("id"), true),
        Arguments.of(Expressions.isNull("data"), Expressions.equal("data", ""), false),
        Arguments.of(Expressions.not(Expressions.equal("id", 5)), Expressions.equal("id", 6), true),
        Arguments.of(
            Expressions.not(Expressions.equal("id", 5)), Expressions.equal("id", 5), false),
        Arguments.of(
            Expressions.startsWith("data", "abc"), Expressions.equal("data", "bcd"), false),
        Arguments.of(
            Expressions.startsWith("data", "abc"), Expressions.equal("data", "abcd"), true),
        Arguments.of(
            Expressions.notStartsWith("data", "abcd"),
            Expressions.notStartsWith("data", "abc"),
            true),
        Arguments.of(
            Expressions.notStartsWith("data", "abc"),
            Expressions.notStartsWith("data", "abcd"),
            false),
        Arguments.of(
            Expressions.notStartsWith("data", "abc"), Expressions.notEqual("data", "abcd"), false),

        // basic range predicates
        Arguments.of(Expressions.greaterThan("id", 5), Expressions.greaterThan("id", 10), true),
        Arguments.of(Expressions.greaterThan("id", 5), Expressions.equal("id", 10), true),
        Arguments.of(Expressions.greaterThan("id", 5), Expressions.equal("id", 5), false),
        Arguments.of(Expressions.greaterThan("id", 10), Expressions.lessThan("id", 5), false),
        Arguments.of(
            Expressions.greaterThanOrEqual("id", 5),
            Expressions.greaterThanOrEqual("id", 10),
            true),
        Arguments.of(Expressions.greaterThanOrEqual("id", 5), Expressions.equal("id", 10), true),
        Arguments.of(Expressions.lessThan("id", 15), Expressions.lessThan("id", 10), true),
        Arguments.of(Expressions.lessThan("id", 15), Expressions.equal("id", 10), true),
        Arguments.of(Expressions.lessThan("id", 15), Expressions.equal("id", 15), false),
        Arguments.of(
            Expressions.lessThanOrEqual("id", 15), Expressions.lessThanOrEqual("id", 10), true),
        Arguments.of(Expressions.lessThanOrEqual("id", 15), Expressions.equal("id", 10), true),

        // set predicates
        Arguments.of(Expressions.in("id", 1, 2, 3), Expressions.equal("id", 2), true),
        Arguments.of(Expressions.notIn("id", 1, 2, 3), Expressions.equal("id", 2), false),
        Arguments.of(Expressions.notIn("id", 1, 2, 3), Expressions.equal("id", 4), true),
        Arguments.of(Expressions.in("id", 1, 2, 3), Expressions.in("id", 1, 2), true),
        Arguments.of(Expressions.in("id", 1, 2, 3), Expressions.in("id", 1, 2, 3), true),
        Arguments.of(Expressions.in("data", "test"), Expressions.startsWith("data", "test"), false),
        Arguments.of(
            Expressions.in("data", "test"), Expressions.notStartsWith("data", "test"), false),
        Arguments.of(Expressions.in("id", 1, 2, 3), Expressions.notIn("id", 3), false),
        Arguments.of(Expressions.notIn("id", 1, 2, 3), Expressions.in("id", 1, 2, 3), false),
        Arguments.of(
            Expressions.notIn("data", "test"), Expressions.notStartsWith("data", "test"), false),
        Arguments.of(
            Expressions.notStartsWith("data", "test"), Expressions.notIn("data", "test"), false),
        Arguments.of(Expressions.notIn("id", 1, 2, 3), Expressions.notIn("id", 1, 2), false),
        Arguments.of(Expressions.notIn("id", 1, 2), Expressions.notIn("id", 1, 2, 3), true),
        Arguments.of(Expressions.notIn("id", 3, 4, 5), Expressions.notEqual("id", 3), false),
        Arguments.of(Expressions.notEqual("id", 3), Expressions.notIn("id", 3, 4, 5), true),

        // logical expressions
        Arguments.of(
            Expressions.or(Expressions.equal("id", 5), Expressions.greaterThan("data", "test")),
            Expressions.equal("id", 5),
            true),
        Arguments.of(
            Expressions.or(Expressions.equal("id", 1), Expressions.equal("data", "test")),
            Expressions.equal("id", 3),
            false),
        Arguments.of(
            Expressions.and(Expressions.equal("id", 1), Expressions.equal("data", "test")),
            Expressions.equal("data", "test"),
            false),
        Arguments.of(
            Expressions.and(Expressions.equal("id", 10), Expressions.equal("data", "test")),
            Expressions.and(Expressions.equal("data", "test"), Expressions.equal("id", 10)),
            true),
        Arguments.of(
            Expressions.or(
                Expressions.lessThanOrEqual("id", 20), Expressions.equal("data", "test")),
            Expressions.lessThanOrEqual("id", 15),
            true),
        Arguments.of(
            Expressions.and(
                Expressions.greaterThanOrEqual("id", 5), Expressions.lessThan("data", "test")),
            Expressions.greaterThanOrEqual("id", 5),
            false),
        Arguments.of(
            Expressions.equal("id", 5),
            Expressions.and(Expressions.greaterThan("id", 5), Expressions.lessThan("id", 10)),
            false),
        Arguments.of(
            Expressions.not(Expressions.greaterThanOrEqual("id", 10)),
            Expressions.lessThan("id", 10),
            true),
        Arguments.of(
            Expressions.or(Expressions.isNull("id"), Expressions.equal("data", "test")),
            Expressions.isNull("id"),
            true),
        Arguments.of(
            Expressions.or(Expressions.notEqual("id", 1), Expressions.notEqual("data", "test")),
            Expressions.not(
                Expressions.and(Expressions.equal("id", 1), Expressions.equal("data", "test"))),
            true),
        Arguments.of(
            Expressions.notNull("id"),
            Expressions.and(Expressions.notNull("id"), Expressions.greaterThan("data", "test")),
            true),
        Arguments.of(Expressions.notEqual("id", 5), Expressions.equal("id", 8), true),
        Arguments.of(
            Expressions.greaterThan("id", 5),
            Expressions.and(Expressions.equal("id", 6), Expressions.equal("data", "test")),
            true),
        Arguments.of(
            Expressions.and(Expressions.lessThan("id", 10), Expressions.greaterThan("id", 5)),
            Expressions.equal("id", 7),
            true),
        Arguments.of(
            Expressions.and(Expressions.lessThan("id", 10), Expressions.greaterThan("id", 5)),
            Expressions.equal("id", 10),
            false),
        Arguments.of(
            Expressions.or(Expressions.equal("id", 5), Expressions.equal("data", "data")),
            Expressions.or(Expressions.equal("id", 5), Expressions.equal("data", "data-b")),
            false),
        Arguments.of(
            Expressions.or(Expressions.equal("id", 5), Expressions.equal("data", "data")),
            Expressions.and(Expressions.equal("id", 5), Expressions.equal("data", "data-b")),
            true),

        // complex and nested Expressions
        Arguments.of(
            Expressions.and(
                Expressions.or(
                    Expressions.and(
                        Expressions.equal("id", 1), Expressions.greaterThan("data", "a")),
                    Expressions.and(Expressions.equal("id", 2), Expressions.lessThan("data", "z"))),
                Expressions.not(Expressions.equal("data", "test"))),
            Expressions.and(
                Expressions.equal("id", 1),
                Expressions.greaterThan("data", "a"),
                Expressions.not(Expressions.equal("data", "test"))),
            true),
        Arguments.of(
            Expressions.and(
                Expressions.or(Expressions.equal("id", 1), Expressions.equal("id", 2)),
                Expressions.greaterThan("data", "a")),
            Expressions.and(Expressions.equal("id", 1), Expressions.greaterThan("data", "a")),
            true),
        Arguments.of(
            Expressions.and(
                Expressions.greaterThanOrEqual("id", 10),
                Expressions.lessThanOrEqual("id", 20),
                Expressions.startsWith("data", "test")),
            Expressions.and(
                Expressions.greaterThanOrEqual("id", 15),
                Expressions.lessThanOrEqual("id", 18),
                Expressions.startsWith("data", "test")),
            true),
        Arguments.of(
            Expressions.not(
                Expressions.and(Expressions.equal("id", 1), Expressions.equal("data", "test"))),
            Expressions.or(Expressions.notEqual("id", 1), Expressions.notEqual("data", "test")),
            true),
        Arguments.of(
            Expressions.and(
                Expressions.greaterThanOrEqual("id", 10), Expressions.lessThanOrEqual("id", 20)),
            Expressions.and(
                Expressions.lessThanOrEqual("id", 20), Expressions.greaterThanOrEqual("id", 10)),
            true),
        Arguments.of(
            Expressions.and(Expressions.greaterThan("id", 10), Expressions.lessThan("id", 20)),
            Expressions.and(Expressions.lessThan("id", 15), Expressions.greaterThan("id", 12)),
            true),

        // equals null safe
        Arguments.of(
            Expressions.greaterThan("id", 5),
            Expressions.and(Expressions.greaterThan("id", 5), Expressions.notNull("id")),
            true),
        Arguments.of(
            Expressions.and(Expressions.greaterThan("id", 5), Expressions.notNull("id")),
            Expressions.greaterThan("id", 5),
            true));
  }

  @ParameterizedTest
  @MethodSource("expressionProvider")
  public void testCanReuseExistingScan(
      Expression existingExpr, Expression newExpr, boolean expectedResult) {
    Expression boundExistingExpr = GlueTestUtil.bindExpression(SCHEMA, existingExpr);
    Expression boundNewExpr = GlueTestUtil.bindExpression(SCHEMA, newExpr);

    assertThat(GlueTableScanUtil.canReuseExistingScanExpression(boundExistingExpr, boundNewExpr))
        .isEqualTo(expectedResult);
  }
}
