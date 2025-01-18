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

package software.amazon.glue.spark.redshift

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.LongType
import software.amazon.glue.spark.redshift.pushdown.querygeneration.RedshiftQuery

case class RedshiftDMLStatementCommand(sqlContext: SQLContext,
                                       relation: RedshiftRelation,
                                       query: RedshiftQuery) extends LeafRunnableCommand {
  override def output: Seq[Attribute] = Seq(AttributeReference("num_affected_rows", LongType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    relation.runDMLFromSQL(query.getStatement())
  }
}
