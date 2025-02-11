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

package software.amazon.glue.s3a;

import static software.amazon.glue.s3a.S3AUtils.getAWSAccessKeys;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.auth.NoAwsCredentialsException;
import software.amazon.glue.s3native.S3xLoginHelper;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Support simple credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

  public static final String NAME
      = "software.amazon.glue.s3a.SimpleAWSCredentialsProvider";
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
  public AWSCredentials getCredentials() {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
      return new BasicAWSCredentials(accessKey, secretKey);
    }
    throw new NoAwsCredentialsException("SimpleAWSCredentialsProvider",
        "No AWS credentials in the Hadoop configuration");
  }

  @Override
  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
