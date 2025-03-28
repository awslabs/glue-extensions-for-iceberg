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
package software.amazon.glue.spark.redshift.data

import org.slf4j.LoggerFactory
import software.amazon.glue.spark.redshift.Parameters.MergedParameters
import RedshiftWrapperType.{DataAPI, RedshiftInterfaceType}
import software.amazon.glue.spark.redshift.Parameters.MergedParameters

private[redshift] object RedshiftWrapperType extends Enumeration {
  type RedshiftInterfaceType = Value
  val DataAPI, JDBC = Value
}

private[redshift] object RedshiftWrapperFactory {
  private val log = LoggerFactory.getLogger(getClass)
  private val jdbcSingleton = RedshiftWrapperFactory(RedshiftWrapperType.JDBC)
  private val dataAPISingleton = RedshiftWrapperFactory(RedshiftWrapperType.DataAPI)

  def apply(params: MergedParameters): RedshiftWrapper = {
    if (params.dataAPICreds) {
      log.debug("Using Data API to communicate with Redshift")
      dataAPISingleton
    } else {
      log.debug("Using JDBC to communicate with Redshift")
      jdbcSingleton
    }
  }

  private def apply(dataInterfaceType: RedshiftInterfaceType): RedshiftWrapper = {
    dataInterfaceType match {
      case DataAPI => new DataApiWrapper()
      case _ => new JDBCWrapper()
    }
  }
}
