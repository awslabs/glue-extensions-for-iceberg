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

import com.amazonaws.auth._
import org.apache.hadoop.conf.Configuration
import Parameters.MergedParameters
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.glue.spark.redshift.Parameters.MergedParameters

import java.net.URI

private[redshift] object AWSCredentialsUtils {

  /**
    * Generates a credentials string for use in Redshift COPY and UNLOAD statements.
    * Favors a configured `aws_iam_role` if available in the parameters.
    */
  def getRedshiftCredentialsString(params: MergedParameters): String = {

    if (params.iamRole.isDefined) {
      s"aws_iam_role=${params.iamRole.get}"
    } else if (params.temporaryAWSCredentials.isDefined) {
      val credentials = params.temporaryAWSCredentials.get
      credentials.getCredentials match {
        case creds: AWSSessionCredentials =>
          s"aws_access_key_id=${creds.getAWSAccessKeyId};" +
            s"aws_secret_access_key=${creds.getAWSSecretKey};token=${creds.getSessionToken}"
        case creds =>
          s"aws_access_key_id=${creds.getAWSAccessKeyId};" +
            s"aws_secret_access_key=${creds.getAWSSecretKey}"
      }
    } else {
      throw new UnsupportedOperationException("Cannot find IAM role or temporary credentials")
    }
  }

  def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }

  def load(params: MergedParameters, hadoopConfiguration: Configuration): AWSCredentialsProvider = {
    // Load the credentials.
    params.temporaryAWSCredentials.getOrElse(loadFromURI(params.rootTempDir, hadoopConfiguration))
  }

  private def loadFromURI(
      tempPath: String,
      hadoopConfiguration: Configuration): AWSCredentialsProvider = {
    // scalastyle:off
    // A good reference on Hadoop's configuration loading / precedence is
    // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md
    // scalastyle:on
    val uri = new URI(tempPath)
    val uriScheme = uri.getScheme

    uriScheme match {
      case "s3" | "s3n" | "software/amazon/glue/s3a" =>
         new DefaultAWSCredentialsProviderChain()
      case other =>
        throw new IllegalArgumentException(s"Unrecognized scheme $other; expected s3, s3n, or s3a")
    }
  }
}
