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

package software.amazon.glue.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

package object redshift {

  /**
   * Wrapper of SQLContext that provide `redshiftFile` method.
   */
  implicit class RedshiftContext(sqlContext: SQLContext) {

    /**
     * Read a file unloaded from Redshift into a DataFrame.
     * @param path input path
     * @return a DataFrame with all string columns
     */
    def redshiftFile(path: String, columns: Seq[String]): DataFrame = {
      val sc = sqlContext.sparkContext
      val rdd = sc.newAPIHadoopFile(path, classOf[RedshiftInputFormat],
        classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
      // TODO: allow setting NULL string.
      val nullable = rdd.values.map(_.map(f => if (f.isEmpty) null else f)).map(x => Row(x: _*))
      val schema = StructType(columns.map(c => StructField(c, StringType, nullable = true)))
      sqlContext.createDataFrame(nullable, schema)
    }

    /**
     * Reads a table unload from Redshift with its schema.
     */
    def redshiftFile(path: String, schema: StructType): DataFrame = {
      val casts = schema.fields.map { field =>
        col(field.name).cast(field.dataType).as(field.name)
      }
      redshiftFile(path, schema.fieldNames).select(casts: _*)
    }
  }
}
