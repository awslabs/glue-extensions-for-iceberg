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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryOperator, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, EqualNullSafe, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.types._
import software.amazon.glue.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException}
import software.amazon.glue.spark.redshift.pushdown.{BooleanVariable, ByteVariable, ConstantString, ConstantStringVal, DoubleVariable, FloatVariable, IntVariable, LongVariable, RedshiftSQLStatement, ShortVariable, StringVariable}
import software.amazon.glue.spark.redshift.RedshiftFailMessage

import scala.language.postfixOps

/**
  * Extractor for basic (attributes and literals) expressions.
  */
private[querygeneration] object BasicStatement {

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
      case a: Attribute => addAttributeStatement(a, fields)
      case Assignment(key, value) =>
          convertStatement(key, fields) + "=" + convertStatement(value, fields)
      case And(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "AND" + convertStatement(
            right,
            fields
          )
        )
      case Or(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "OR" + convertStatement(
            right,
            fields
          )
        )
      case BitwiseAnd(left, right) => null
      case BitwiseOr(left, right) => null
      case BitwiseXor(left, right) => null
      case BitwiseNot(child) => null
      case EqualNullSafe(left, right) => null
      case b: BinaryOperator =>
        blockStatement(
          convertStatement(b.left, fields) + b.symbol + convertStatement(
            b.right,
            fields
          )
        )
      case l: Literal =>
        l.dataType match {
          case StringType =>
            if (l.value == null) {
              ConstantString("NULL") !
            } else {
              StringVariable(Some(l.toString().replace("'", "''"))) ! // else "'" + str + "'"
            }
          case DateType =>
            ConstantString("DATEADD(day,") + IntVariable(
              Option(l.value).map(_.asInstanceOf[Int])
            ) +
              ", TO_DATE('1970-01-01', 'YYYY-MM-DD'))"
          case TimestampType =>
            StringVariable(Option(l.toString)) + "::TIMESTAMPTZ"
          case TimestampNTZType =>
            StringVariable(Option(l.toString)) + "::TIMESTAMP"
          case _: StructType | _: MapType | _: ArrayType =>
            throw new RedshiftPushdownUnsupportedException(
              RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
              s"${expr.prettyName} ${l.dataType} @ BasicStatement",
              expr.sql,
              true)
          case _ =>
            l.value match {
              case v: Int => IntVariable(Some(v)) !
              case v: Long => LongVariable(Some(v)) !
              case v: Short => ShortVariable(Some(v)) !
              case v: Boolean => BooleanVariable(Some(v)) !
              case v: Float => FloatVariable(Some(v)) + "::float4"
              case v: Double => DoubleVariable(Some(v)) !
              case v: Byte => ByteVariable(Some(v)) !
              case _ => ConstantStringVal(l.value) !
            }
        }
      case _ => null
    })
  }
}
