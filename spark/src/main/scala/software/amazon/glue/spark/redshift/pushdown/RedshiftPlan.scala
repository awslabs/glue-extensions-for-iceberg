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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/** RedshiftPlan, with RDD defined by custom query. */
case class RedshiftPlan(output: Seq[Attribute], rdd: RDD[InternalRow])
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Nil
  protected override def doExecute(): RDD[InternalRow] = {
    rdd
  }

  override def simpleString(maxFields: Int): String = {
      super.simpleString(maxFields) + " " + output.mkString("[", ",", "]")
  }

  override def simpleStringWithNodeId(): String = {
    super.simpleStringWithNodeId() + " " + output.mkString("[", ",", "]")
  }

  // withNewChildrenInternal() is a new interface function from spark 3.2 in
  // org.apache.spark.sql.catalyst.trees.TreeNode. For details refer to
  // https://github.com/apache/spark/pull/32030
  // As for spark connector the RedshiftPlan is a leaf Node, we don't expect
  // caller to set any new children for it.
  // RedshiftPlan is only used for spark connector PushDown. Even if the Exception is
  // raised, the PushDown will not be used and it still works.
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    if (newChildren.nonEmpty) {
      throw new Exception("Spark connector internal error: " +
        "RedshiftPlan.withNewChildrenInternal() is called to set some children nodes.")
    }
    this
  }
}