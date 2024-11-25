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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.auth.NoAwsCredentialsException;

import java.io.IOException;
import java.net.URI;

import static software.amazon.glue.s3a.S3AUtils.getAWSAccessKeys;

/**
 * Support simple credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SimpleAWSCredentialsProvider implements AwsCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  private final String accessKey;
  private final String secretKey;

  /**
   * Build the credentials from a filesystem URI and configuration.
   * @param uri FS URI
   * @param conf configuration containing secrets/references to.
   * @throws IOException failure
   */
  public SimpleAWSCredentialsProvider(final URI uri, final Configuration conf)
      throws IOException {
    this(getAWSAccessKeys(uri, conf));
  }

  /**
   * Instantiate from a login tuple.
   * For testing, hence package-scoped.
   * @param login login secrets
   * @throws IOException failure
   */
  @VisibleForTesting
  SimpleAWSCredentialsProvider(final S3xLoginHelper.Login login)
      throws IOException {
    this.accessKey = login.getUser();
    this.secretKey = login.getPassword();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
      return AwsBasicCredentials.create(accessKey, secretKey);
    }
    throw new NoAwsCredentialsException("SimpleAWSCredentialsProvider",
        "No AWS credentials in the Hadoop configuration");
  }

  @Override
  public String toString() {
    return "SimpleAWSCredentialsProvider{" +
        "accessKey.empty=" + accessKey.isEmpty() +
        ", secretKey.empty=" + secretKey.isEmpty() +
        '}';
  }

}
