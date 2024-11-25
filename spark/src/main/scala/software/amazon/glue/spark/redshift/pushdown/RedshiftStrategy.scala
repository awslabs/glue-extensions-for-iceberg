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

package software.amazon.glue.spark.redshift.pushdown

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}
import org.apache.spark.sql.{SparkSession, Strategy}
import software.amazon.glue.spark.redshift.RedshiftRelation
import software.amazon.glue.spark.redshift.pushdown.querygeneration.QueryBuilder

import scala.collection.mutable.ArrayBuffer

/**
 * Clean up the plan, then try to generate a query from it for Redshift.
 */
case class RedshiftStrategy(session: SparkSession) extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      log.info(s"Trying to pushdown the Spark optimized plan ${plan}")
      QueryBuilder.getSparkPlanFromLogicalPlan(plan.transform({
        case Project(Nil, child) => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {

      case t: UnsupportedOperationException =>
        log.warn(s"Unsupported Operation:${t.getMessage}")
        Nil

      case e: Exception =>
        log.warn(s"Pushdown failed:${e.getMessage}", e)
        Nil
    }
  }
}