package software.amazon.glue.spark.redshift.pushdown

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Row, SparkSession}
import software.amazon.glue.spark.redshift.RedshiftRelation


case class RedshiftDMLExec (query: RedshiftSQLStatement, relation: RedshiftRelation)
  extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq(AttributeReference("num_affected_rows", LongType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    relation.runDMLFromSQL(query)
  }
}
