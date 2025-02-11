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

import org.apache.iceberg.GlueTable
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DeleteFromTable, InsertIntoStatement, LogicalPlan, MergeIntoTable, Project, ReplaceData, SubqueryAlias, UpdateTable, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import software.amazon.glue.GlueExtensionsTableOperations
import software.amazon.glue.spark.redshift.pushdown.querygeneration.QueryBuilder

import scala.collection.JavaConverters._

case class ConvertIcebergToRedshiftRelation(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  private[redshift] val redshiftRelationProvider = new DefaultSource

  override def apply(plan: LogicalPlan): LogicalPlan = {
    tryConvertToRedshiftDMLStatementCommand(plan).map(result => {
      if (result.isInstanceOf[RedshiftDMLStatementCommand]) {
        result
      } else {
        convertAllRedshiftRelation(plan)
      }
    }).getOrElse(plan)
  }

  private def convertAllRedshiftRelation(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {

      case l@InsertIntoStatement(v2Relation, arg1, arg2, arg3, arg4, arg5, arg6) =>
        v2Relation match {
          case DataSourceV2Relation(table, output, _, _, _) =>
            table match {
              case icebergTable: SparkTable if IcebergUtils.serverSideDataCommitEnabled(icebergTable) =>
                icebergTable.table() match {
                  case glueTable: GlueTable =>
                    val redshiftRelation = createRedshiftRelation(icebergTable, glueTable)
                    val logicalRelation = LogicalRelation(redshiftRelation, output, None, isStreaming = false)
                    InsertIntoStatement(logicalRelation, arg1, arg2, arg3, arg4, arg5, arg6)
                  case _ =>
                    l
                }
              case _ =>
                l
            }
          case _ =>
            l
        }

      case l@DataSourceV2Relation(table, output, _, _, _) =>
        table match {
          case icebergTable: SparkTable if IcebergUtils.serverSideScanPlanningEnabled(icebergTable) =>
            icebergTable.table() match {
              case glueTable: GlueTable =>
                val redshiftRelation = createRedshiftRelation(icebergTable, glueTable)
                LogicalRelation(redshiftRelation, output, None, isStreaming = false)
              case _ =>
                l
            }
          case _ =>
            l
        }
    }
  }

  private def tryConvertToRedshiftDMLStatementCommand(plan: LogicalPlan): Option[LogicalPlan] = {
    var dmlPushdownFailed = false

    val resultPlan = eliminateSubqueryAliasesAndView(plan) transformDown {

      case l @ InsertIntoStatement(DataSourceV2Relation(table, output, _, _, _), _, _, query, overwrite, _, _) =>
        table match {
          case icebergTable: SparkTable if IcebergUtils.serverSideDataCommitEnabled(icebergTable) =>
            val glueTable = icebergTable.table().asInstanceOf[GlueTable]
            val glueRedshiftRelation = createRedshiftRelation(icebergTable, glueTable)
            val convertedQuery = query transform {
              case DataSourceV2Relation(table, output, _, _, _) =>
                table match {
                  case icebergTable: SparkTable if IcebergUtils.serverSideScanPlanningEnabled(icebergTable) =>
                    val redshiftRelation = createRedshiftRelation(icebergTable, icebergTable.table().asInstanceOf[GlueTable])
                    LogicalRelation(redshiftRelation, output, None, isStreaming = false)
                }
            }
            // same logic as org.apache.spark.sql.execution.datasources.DataSourceStrategy
            val convertedPlan = InsertIntoDataSourceCommand(
              LogicalRelation(glueRedshiftRelation, output, None, isStreaming = false),
              convertedQuery,
              overwrite)
            tryConvertToRedshiftCommand(glueTable, convertedPlan, glueRedshiftRelation).getOrElse {
              dmlPushdownFailed = true
              l
            }
          case _ =>
            dmlPushdownFailed = true
            l
        }

      case l @ DeleteFromTable(DataSourceV2Relation(table, output, _, _, _), condition) =>
        table match {
          case icebergTable: SparkTable if IcebergUtils.serverSideDataCommitEnabled(icebergTable) =>
            val glueTable = icebergTable.table().asInstanceOf[GlueTable]
            val glueRedshiftRelation = createRedshiftRelation(icebergTable, glueTable)
            val logicalRelation = LogicalRelation(glueRedshiftRelation, output, None, isStreaming = false)
            val convertedPlan = DeleteFromTable(logicalRelation, condition)
            tryConvertToRedshiftCommand(glueTable, convertedPlan, glueRedshiftRelation).getOrElse {
              dmlPushdownFailed = true
              l
            }
          case _ =>
            dmlPushdownFailed = true
            l
        }

      case l @ UpdateTable(DataSourceV2Relation(table, output, _, _, _), assignments, condition) =>
        table match {
          case icebergTable: SparkTable if IcebergUtils.serverSideDataCommitEnabled(icebergTable) =>
            val glueTable = icebergTable.table().asInstanceOf[GlueTable]
            val glueRedshiftRelation = createRedshiftRelation(icebergTable, glueTable)
            val logicalRelation = LogicalRelation(glueRedshiftRelation, output, None, isStreaming = false)
            val convertedPlan = UpdateTable(logicalRelation, assignments, condition)
            tryConvertToRedshiftCommand(glueTable, convertedPlan, glueRedshiftRelation).getOrElse {
              dmlPushdownFailed = true
              l
            }
          case _ =>
            dmlPushdownFailed = true
            l
        }

      case l @ MergeIntoTable(DataSourceV2Relation(table, output, _, _, _), sourceTable, mergeCondition, matchedActions, notMatchedActions, notMatchedBySourceActions) =>
        table match {
          case icebergTable: SparkTable if IcebergUtils.serverSideDataCommitEnabled(icebergTable) =>
            val glueTable = icebergTable.table().asInstanceOf[GlueTable]
            val glueRedshiftRelation = createRedshiftRelation(icebergTable, glueTable)
            val logicalRelation = LogicalRelation(glueRedshiftRelation, output, None, isStreaming = false)
            val convertedSourceTable = sourceTable transform {
              case DataSourceV2Relation(table, output, _, _, _) =>
                table match {
                  case icebergTable: SparkTable if IcebergUtils.serverSideScanPlanningEnabled(icebergTable) =>
                    val redshiftRelation = createRedshiftRelation(icebergTable, icebergTable.table().asInstanceOf[GlueTable])
                    LogicalRelation(redshiftRelation, output, None, isStreaming = false)
                }
            }
            val convertedPlan = MergeIntoTable(logicalRelation, convertedSourceTable, mergeCondition, matchedActions, notMatchedActions, notMatchedBySourceActions)
            tryConvertToRedshiftCommand(glueTable, convertedPlan, glueRedshiftRelation).getOrElse {
              dmlPushdownFailed = true
              l
            }
          case _ =>
            dmlPushdownFailed = true
            l
        }

      case l : AppendData =>
        dmlPushdownFailed = true
        l

      case l : ReplaceData =>
        dmlPushdownFailed = true
        l

      case default => default
    }

    if (dmlPushdownFailed) {
      Option.empty
    } else {
      Option.apply(resultPlan)
    }
  }


  private def tryConvertToRedshiftCommand(glueTable: GlueTable,
                                            plan: LogicalPlan,
                                            redshiftRelation: RedshiftRelation): Option[RedshiftDMLStatementCommand] = {
    val ops = glueTable.operations().asInstanceOf[GlueExtensionsTableOperations]
    if (ops.properties().debugEnabled()) {
      logInfo(s"Try convert plan: $plan")
    }
    val qb = new QueryBuilder(plan)

    qb.tryBuild.map { executedBuilder =>
      // trigger statement invocation once to make sure the SQL can be produced
      try {
        val query = executedBuilder.treeRoot
        query.getStatement()
        if (ops.properties().debugEnabled()) {
          logInfo(s"Try convert to statement ${executedBuilder.statement}")
        } else {
          executedBuilder.statement
        }
        val statementRelation = RedshiftDMLStatementCommand(
          spark.sqlContext,
          redshiftRelation,
          executedBuilder.treeRoot)
        logInfo("Conversion succeeded")
        Option(statementRelation)
      } catch {
        case e: Throwable =>
          logError("Plan conversion test failed with statement conversion", e)
          None
      }
    } getOrElse {
      logInfo(s"Failed to convert plan")
      None
    }
  }

  private def createRedshiftRelation(icebergTable: SparkTable, glueTable: GlueTable): RedshiftRelation = {
    val ops = glueTable.operations().asInstanceOf[GlueExtensionsTableOperations]
    val configs = ops.redshiftConnectionConfigs().asScala.toMap
    if (ops.properties().debugEnabled()) {
      logInfo(s"Convert Iceberg GlueTable to Redshift relation with configs: $configs")
    }

    redshiftRelationProvider
      .createRelation(spark.sqlContext, CaseInsensitiveMap(configs), icebergTable.schema()).asInstanceOf[RedshiftRelation]
  }

  private def eliminateSubqueryAliasesAndView(plan: LogicalPlan): LogicalPlan = {
    val transformed = plan.transformWithSubqueries {
      case SubqueryAlias(_, child) => child
      case View(_, _, child) => child
    }
    if (transformed != plan) eliminateSubqueryAliasesAndView(transformed) else transformed
  }
}
