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

import org.apache.spark.util.AccumulatorV2

class SetAccumulator[T](var value: Set[T]) extends AccumulatorV2[T, Set[T]] {
  def this() = this(Set.empty[T])
  override def isZero: Boolean = value.isEmpty
  override def copy(): AccumulatorV2[T, Set[T]] = new SetAccumulator[T](value)
  override def reset(): Unit = value = Set.empty[T]
  override def add(v: T): Unit = value = value + v
  override def merge(other: AccumulatorV2[T, Set[T]]): Unit =
    value = value ++ other.value
}
