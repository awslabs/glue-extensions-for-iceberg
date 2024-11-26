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
