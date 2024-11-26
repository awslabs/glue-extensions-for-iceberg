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

package software.amazon.glue.s3a.auth.delegation;

import java.net.URI;

import software.amazon.glue.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.io.Text;

/**
 * Role token identifier.
 * Token kind is {@link DelegationConstants#ROLE_TOKEN_KIND}
 */
public class RoleTokenIdentifier extends SessionTokenIdentifier {

  public RoleTokenIdentifier() {
    super(DelegationConstants.ROLE_TOKEN_KIND);
  }

  public RoleTokenIdentifier(final URI uri,
      final Text owner,
      final Text renewer,
      final MarshalledCredentials marshalledCredentials,
      final EncryptionSecrets encryptionSecrets,
      final String origin) {
    super(DelegationConstants.ROLE_TOKEN_KIND,
        owner,
        renewer,
        uri,
        marshalledCredentials,
        encryptionSecrets,
        origin);
  }

}
