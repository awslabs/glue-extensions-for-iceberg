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

package software.amazon.glue.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Asin, Atan, Attribute, Ceil, CheckOverflow, Cos, Exp, Expression, Floor, Greatest, Least, Log10, Pi, Pow, Sin, Sqrt, Tan, UnaryMinus}
import software.amazon.glue.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}

import scala.language.postfixOps

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object NumericStatement {

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Abs | _: Acos | _: Cos | _: Tan | _: Atan |
          _: Floor | _: Sin | _: Asin | _: Sqrt | _: Ceil |
          _: Sqrt | _: Greatest | _: Least | _: Exp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case _: Log10 =>
        ConstantString("LOG") +
          blockStatement(convertStatements(fields, expr.children: _*))

      // From spark 3.1, UnaryMinus() has 2 parameters.
      case UnaryMinus(child, _) =>
        ConstantString("-") +
          blockStatement(convertStatement(child, fields))

      case Pow(left, right) =>
        ConstantString("POWER") +
          blockStatement(
            convertStatement(left, fields) + "," + convertStatement(
              right,
              fields
            )
          )

      case PromotePrecisionExtractor(child) => convertStatement(child, fields)

      case CheckOverflow(child, t, _) =>
        getCastType(t) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }

      // Spark has resolved PI() as 3.141592653589793
      // Suppose connector can't see Pi().
      case Pi() => ConstantString("PI()") !

      case RoundExtractor(child, scale, ansiEnabled) if !ansiEnabled =>
        ConstantString("ROUND") + blockStatement(
          convertStatements(fields, child, scale)
        )

      case _ => null
    })
  }
}