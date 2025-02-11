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

import com.amazonaws.auth.AWSCredentialsProvider;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;

/**
 * Base class for AWS credential providers which
 * take a URI and config in their constructor.
 */
public abstract class AbstractAWSCredentialProvider
    implements AWSCredentialsProvider {

  private final URI binding;

  private final Configuration conf;

  /**
   * Construct from URI + configuration.
   * @param uri URI: may be null.
   * @param conf configuration.
   */
  protected AbstractAWSCredentialProvider(
      @Nullable final URI uri,
      final Configuration conf) {
    this.conf = conf;
    this.binding = uri;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the binding URI: may be null.
   * @return the URI this instance was constructed with,
   * if any.
   */
  public URI getUri() {
    return binding;
  }

  /**
   * Refresh is a no-op by default.
   */
  @Override
  public void refresh() {
  }
}
