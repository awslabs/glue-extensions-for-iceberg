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

import software.amazon.glue.spark.redshift.Parameters.MergedParameters

import java.sql.Connection
import scala.collection.mutable.ArrayBuffer

private[redshift] abstract class RedshiftConnection() {
  def getAutoCommit(): Boolean
  def setAutoCommit(autoCommit: Boolean): Unit
  def close(): Unit
}

private[redshift] case class JDBCConnection(conn: Connection) extends RedshiftConnection {
  override def getAutoCommit(): Boolean = {
    conn.getAutoCommit()
  }

  override def setAutoCommit(autoCommit: Boolean): Unit = {
    conn.setAutoCommit(autoCommit)
  }

  override def close(): Unit = {
    conn.close()
  }
}

private[redshift] case class DataAPIConnection(params: MergedParameters,
                                               queryGroup: Option[String] = None,
                                              ) extends RedshiftConnection {
  val bufferedCommands: ArrayBuffer[String] = ArrayBuffer.empty
  var autoCommit: Boolean = true

  override def getAutoCommit(): Boolean = {
    autoCommit
  }

  override def setAutoCommit(autoCommit: Boolean): Unit = {
    this.autoCommit = autoCommit
  }

  override def close(): Unit = {
    // Reset in case someone tries to reuse this object. However, this object should
    // not be used after closing. We may want to enforce this at some point.
    bufferedCommands.clear()
    autoCommit = true
  }
}