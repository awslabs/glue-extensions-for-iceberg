/*
* Copyright 2015-2018 Snowflake Computing
* Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package software.amazon.glue.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan}

/** Extractor for binary logical operations (e.g., joins). */
private[querygeneration] object BinaryOp {

  def unapply(node: BinaryNode): Option[(LogicalPlan, LogicalPlan)] =
    Option(node match {
      case _: Join => (node.left, node.right)
      case _ => null
    })
}
