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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
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
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class AnonymousAWSCredentialsProvider implements AWSCredentialsProvider {

  public static final String NAME
      = "software.amazon.glue.s3a.AnonymousAWSCredentialsProvider";

  public AWSCredentials getCredentials() {
    return new AnonymousAWSCredentials();
  }

  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
