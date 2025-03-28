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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import Parameters.{DEFAULT_PARAMETERS, PARAM_OVERRIDE_NULLABLE}

/**
 * Internal data source used for reading Redshift UNLOAD files.
 *
 * This is not intended for public consumption / use outside of this package and therefore
 * no API stability is guaranteed.
 */
private[redshift] class RedshiftFileFormat extends FileFormat {
  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    // Schema is provided by caller.
    None
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(s"prepareWrite is not supported for $this")
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    // Our custom InputFormat handles split records properly
    true
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    require(partitionSchema.isEmpty)
    require(filters.isEmpty)
    require(dataSchema == requiredSchema)

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      val fileSplit = new FileSplit(
        new Path(RedshiftFileFormatUtils.uriFromPartitionedFile(file)),
        file.start,
        file.length,
        // TODO: Implement Locality
        Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      val reader = new RedshiftRecordReader
      reader.initialize(fileSplit, hadoopAttemptContext)
      val iter = new RecordReaderIterator[Array[String]](reader)

      // Ensure that the record reader is closed upon task completion. It will ordinarily
      // be closed once it is completely iterated, but this is necessary to guard against
      // resource leaks in case the task fails or is interrupted.
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      val converter = Conversions.createRowConverter(requiredSchema,
        options.getOrElse("nullString", DEFAULT_PARAMETERS("csvnullstring")),
        options.getOrElse(PARAM_OVERRIDE_NULLABLE,
          DEFAULT_PARAMETERS(PARAM_OVERRIDE_NULLABLE)).toBoolean)
      iter.map(converter)
    }
  }

  override def supportDataType(dataType: DataType): Boolean = {
    dataType match {
      case ByteType | BooleanType | DateType | DoubleType | FloatType | IntegerType => true
      case LongType | ShortType | StringType | TimestampType => true
      case _ : DecimalType => true
      case _ => false
    }
  }
}
