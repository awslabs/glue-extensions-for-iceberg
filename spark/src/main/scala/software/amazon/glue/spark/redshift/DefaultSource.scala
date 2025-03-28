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

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import Parameters.MergedParameters
import software.amazon.glue.spark.redshift.data.RedshiftWrapperFactory
import software.amazon.glue.spark.redshift.pushdown.RedshiftStrategy

/**
 * Redshift Source implementation for Spark SQL
 */
class DefaultSource(s3ClientFactory: (AWSCredentialsProvider, MergedParameters) => AmazonS3)
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Default constructor required by Data Source API
   */
  def this() = this(Utils.s3ClientBuilder)

  /**
   * Create a new RedshiftRelation instance using parameters from Spark SQL DDL. Resolves the schema
   * using JDBC connection over provided URL, which must contain credentials.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Load a RedshiftRelation using user-provided schema, so no inference over JDBC will be used.
   */
  override def createRelation( sqlContext: SQLContext,
                               userParameters: Map[String, String],
                               schema: StructType): BaseRelation = {
    val mergedParams = Parameters.mergeParameters(userParameters)

    RedshiftRelation(
      RedshiftWrapperFactory(mergedParams),
      mergedParams, Option(schema))(sqlContext)
  }

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to Redshift
   */
  override def createRelation( sqlContext: SQLContext,
                               saveMode: SaveMode,
                               userParameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    val mergedParams = Parameters.mergeParameters(userParameters)
    val table = mergedParams.table.getOrElse {
      throw new IllegalArgumentException(
        "For save operations you must specify a Redshift table name with the 'dbtable' parameter")
    }
    val redshiftWrapper = RedshiftWrapperFactory(mergedParams)

    def tableExists: Boolean = {
      val conn = redshiftWrapper.getConnector(mergedParams)
      try {
        redshiftWrapper.tableExists(conn, table.toString)
      } finally {
        conn.close()
      }
    }

    val (doSave, dropExisting) = saveMode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if (tableExists) {
          sys.error(s"Table $table already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if (tableExists) {
          log.info(s"Table $table already exists -- ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if (doSave) {
      val updatedUserParameters = userParameters.updated("overwrite", dropExisting.toString)
      val mergedParameters = Parameters.mergeParameters(updatedUserParameters)
      new RedshiftWriter(redshiftWrapper).saveToRedshift(
        sqlContext, data, saveMode, mergedParameters)
    }

    createRelation(sqlContext, userParameters)
  }

//  def enablePushdownSession(session: SparkSession): Unit = {
//
//    if (!session.experimental.extraStrategies.exists(
//      s => s.isInstanceOf[RedshiftStrategy]
//    )) {
//      log.info("Enable auto pushdown.")
//      session.experimental.extraStrategies ++= Seq(RedshiftStrategy(session))
//    }
//  }
}
