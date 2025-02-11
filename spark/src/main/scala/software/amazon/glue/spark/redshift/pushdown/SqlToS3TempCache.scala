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

package software.amazon.glue.spark.redshift.pushdown

import java.util.concurrent.ConcurrentHashMap

object SqlToS3TempCache {
  private val sqlToS3Cache = new ConcurrentHashMap[String, String]()

  def getS3Path(sql : String): Option[String] = {
    Option(sqlToS3Cache.get(sql))
  }

  def setS3Path(sql : String, s3Path : String): Option[String] = {
    Option(sqlToS3Cache.put(sql, s3Path))
  }

  def clearCache(): Unit = {
    sqlToS3Cache.clear()
  }

}
