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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, ToPrettyString}
import org.apache.spark.sql.types._
import software.amazon.glue.spark.redshift.pushdown.{ConstantString, RedshiftSQLStatement}

private[querygeneration] object StringStatementExtensions {
  def unapply(expAttr: (Expression, Seq[Attribute])): Option[RedshiftSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {

      case ToPrettyString(child: Expression, timeZoneId: Option[String]) =>
        ConstantString("CASE WHEN") +
          convertStatement(child, fields) +
          ConstantString("IS NULL THEN 'NULL' ELSE") +
          convertStatement(Cast(child, StringType, timeZoneId), fields) +
          ConstantString("END")

      case _ => null
    })
  }
}
