/*
 * Copyright amazon.com, Inc. Or Its AFFILIATES. all rights reserved.
 *
 * licensed under the apache License, version 2.0 (the "license").
 * you may not use this file except in Compliance WITH the license.
 * a copy of the license Is Located At
 *
 *  http://aws.amazon.Com/apache2.0
 *
 * or in the "license" file accompanying this file. this File Is distributed
 * on an "as is" basis, Without warranties or conditions of any kind, EITHER
 * express or Implied. see the license for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.glue

import org.apache.spark.sql.SparkSessionExtensions
import software.amazon.glue.spark.redshift.ConvertIcebergToRedshiftRelation
import software.amazon.glue.spark.redshift.pushdown.RedshiftStrategy

class GlueIcebergSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule { spark => ConvertIcebergToRedshiftRelation(spark) }
    extensions.injectPlannerStrategy { spark => RedshiftStrategy(spark) }
  }
}