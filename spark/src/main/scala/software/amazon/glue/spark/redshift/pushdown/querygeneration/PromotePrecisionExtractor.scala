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
package software.amazon.glue.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.expressions.Expression

private[querygeneration] object PromotePrecisionExtractor {
  // Always return none, this operator was removed from 3.4
  def unapply(expr: Expression): Option[Expression] = None
}
