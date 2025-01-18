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

import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object ScalarSubqueryExtractor {
  def unapply(expr: Expression): Option[(LogicalPlan, Seq[Expression], ExprId, Seq[Expression])] =
    expr match {
      // ignoring hintinfo and mayHaveCountBug
      case sq : ScalarSubquery =>
        Some(sq.plan, sq.outerAttrs, sq.exprId, sq.joinCond)
      case _ => None
  }
}
