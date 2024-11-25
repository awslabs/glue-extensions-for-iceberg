package software.amazon.glue.spark.redshift

import org.apache.iceberg.GlueTable
import org.apache.iceberg.spark.source.SparkTable

object IcebergUtils {

  def serverSideScanPlanningEnabled(icebergTable: SparkTable): Boolean = {
    icebergTable.table().isInstanceOf[GlueTable] &&
      icebergTable.table().asInstanceOf[GlueTable].serverSideScanPlanningEnabled()
  }

  def serverSideDataCommitEnabled(icebergTable: SparkTable): Boolean = {
    icebergTable.table().isInstanceOf[GlueTable] &&
      icebergTable.table().asInstanceOf[GlueTable].serverSideScanPlanningEnabled()
  }

}
