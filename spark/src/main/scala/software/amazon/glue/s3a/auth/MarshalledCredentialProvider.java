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

import static software.amazon.glue.s3a.auth.MarshalledCredentialBinding.toAWSCredentials;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.auth.AWSCredentials;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.CredentialInitializationException;

/**
 * AWS credential provider driven from marshalled session/full credentials
 * (full, simple session or role).
 * This is <i>not</i> intended for explicit use in job/app configurations,
 * instead it is returned by Delegation Token Bindings, as needed.
 * The constructor implicitly prevents explicit use.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MarshalledCredentialProvider extends
    AbstractSessionCredentialsProvider {

  /** Name: {@value}. */
  public static final String NAME
      = "software.amazon.glue.s3a.auth.MarshalledCredentialProvider";

  private final MarshalledCredentials credentials;

  private final MarshalledCredentials.CredentialTypeRequired typeRequired;

  private final String component;

  /**
   * Constructor.
   *
   * @param component component name for exception messages.
   * @param uri filesystem URI: must not be null.
   * @param conf configuration.
   * @param credentials marshalled credentials.
   * @param typeRequired credential type required.
   * @throws CredentialInitializationException validation failure
   * @throws IOException failure
   */
  public MarshalledCredentialProvider(
      final String component,
      final URI uri,
      final Configuration conf,
      final MarshalledCredentials credentials,
      final MarshalledCredentials.CredentialTypeRequired typeRequired)
      throws IOException {
    super(checkNotNull(uri, "No filesystem URI"), conf);
    this.component = component;
    this.typeRequired = typeRequired;
    this.credentials = checkNotNull(credentials);
  }

  /**
   * Perform the binding, looking up the DT and parsing it.
   * @return true if there were some credentials
   * @throws CredentialInitializationException validation failure
   * @throws IOException on a failure
   */
  @Override
  protected AWSCredentials createCredentials(final Configuration config)
      throws IOException {
    return toAWSCredentials(credentials, typeRequired, component);
  }

}
