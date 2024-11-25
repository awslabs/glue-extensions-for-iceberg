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

package software.amazon.glue.s3a;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AnonymousAWSCredentialsProvider supports anonymous access to AWS services
 * through the AWS SDK.  AWS requests will not be signed.  This is not suitable
 * for most cases, because allowing anonymous access to an S3 bucket compromises
 * security.  This can be useful for accessing public data sets without
 * requiring AWS credentials.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class AnonymousAWSCredentialsProvider implements AwsCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";

  public AwsCredentials resolveCredentials() {
    return AnonymousCredentialsProvider.create().resolveCredentials();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
