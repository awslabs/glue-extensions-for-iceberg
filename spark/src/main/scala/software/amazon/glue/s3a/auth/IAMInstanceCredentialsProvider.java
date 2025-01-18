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

package software.amazon.glue.s3a.auth;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is an IAM credential provider which wraps
 * an {@code EC2ContainerCredentialsProviderWrapper}
 * to provide credentials when the S3A connector is instantiated on AWS EC2
 * or the AWS container services.
 * <p>
 * When it fails to authenticate, it raises a
 * {@link NoAwsCredentialsException} which can be recognized by retry handlers
 * as a non-recoverable failure.
 * <p>
 * It is implicitly public; marked evolving as we can change its semantics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IAMInstanceCredentialsProvider
    implements AWSCredentialsProvider, Closeable {

  private final AWSCredentialsProvider provider =
      new EC2ContainerCredentialsProviderWrapper();

  public IAMInstanceCredentialsProvider() {
  }

  /**
   * Ask for the credentials.
   * Failure invariably means "you aren't running in an EC2 VM or AWS container".
   * @return the credentials
   * @throws NoAwsCredentialsException on auth failure to indicate non-recoverable.
   */
  @Override
  public AWSCredentials getCredentials() {
    try {
      return provider.getCredentials();
    } catch (AmazonClientException e) {
      throw new NoAwsCredentialsException("IAMInstanceCredentialsProvider",
          e.getMessage(),
          e);
    }
  }

  @Override
  public void refresh() {
    provider.refresh();
  }

  @Override
  public void close() throws IOException {
    // no-op.
  }
}
