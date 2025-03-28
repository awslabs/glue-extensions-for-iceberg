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

import static software.amazon.glue.s3a.auth.MarshalledCredentialBinding.fromSTSCredentials;
import static software.amazon.glue.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_CREDENTIALS_PROVIDER;
import static software.amazon.glue.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_ROLE_ARN;
import static software.amazon.glue.s3a.auth.delegation.DelegationConstants.E_NO_SESSION_TOKENS_FOR_ROLE_BINDING;

import com.amazonaws.services.securitytoken.model.Credentials;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.AWSCredentialProviderList;
import software.amazon.glue.s3a.Retries;
import software.amazon.glue.s3a.auth.MarshalledCredentialProvider;
import software.amazon.glue.s3a.auth.MarshalledCredentials;
import software.amazon.glue.s3a.auth.RoleModel;
import software.amazon.glue.s3a.auth.STSClientFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Role Token support requests an explicit role and automatically restricts
 * that role to the given policy of the binding.
 * The session is locked down as much as possible.
 */
public class RoleTokenBinding extends SessionTokenBinding {

  private static final Logger LOG = LoggerFactory.getLogger(
      RoleTokenBinding.class);

  private static final RoleModel MODEL = new RoleModel();

  /**
   * Wire name of this binding includes a version marker: {@value}.
   */
  private static final String NAME = "RoleCredentials/001";

  /**
   * Error message when there is no Role ARN.
   */
  @VisibleForTesting
  public static final String E_NO_ARN =
      "No role ARN defined in " + DELEGATION_TOKEN_ROLE_ARN;

  public static final String COMPONENT = "Role Delegation Token";

  /**
   * Role ARN to use when requesting new tokens.
   */
  private String roleArn;

  /**
   * Constructor.
   * Name is {@link #NAME}; token kind is
   * {@link DelegationConstants#ROLE_TOKEN_KIND}.
   */
  public RoleTokenBinding() {
    super(NAME, DelegationConstants.ROLE_TOKEN_KIND);
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    roleArn = getConfig().getTrimmed(DELEGATION_TOKEN_ROLE_ARN, "");
  }

  /**
   * Returns a (wrapped) {@link MarshalledCredentialProvider} which
   * requires the marshalled credentials to contain session secrets.
   * @param retrievedIdentifier the incoming identifier.
   * @return the provider chain.
   * @throws IOException on failure
   */
  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier)
      throws IOException {
    RoleTokenIdentifier tokenIdentifier =
        convertTokenIdentifier(retrievedIdentifier,
            RoleTokenIdentifier.class);
    setTokenIdentifier(Optional.of(tokenIdentifier));
    MarshalledCredentials marshalledCredentials
        = tokenIdentifier.getMarshalledCredentials();
    setExpirationDateTime(marshalledCredentials.getExpirationDateTime());
    return new AWSCredentialProviderList(
        "Role Token Binding",
        new MarshalledCredentialProvider(
            COMPONENT,
            getStoreContext().getFsURI(),
            getConfig(),
            marshalledCredentials,
            MarshalledCredentials.CredentialTypeRequired.SessionOnly));
  }

  /**
   * Create the Token Identifier.
   * Looks for the option {@link DelegationConstants#DELEGATION_TOKEN_ROLE_ARN}
   * in the config and fail if it is not set.
   * @param policy the policy which will be used for the requested token.
   * @param encryptionSecrets encryption secrets.
   * @return the token.
   * @throws IllegalArgumentException if there is no role defined.
   * @throws IOException any problem acquiring the role.
   */
  @Override
  @Retries.RetryTranslated
  public RoleTokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer) throws IOException {
    requireServiceStarted();
    Preconditions.checkState(!roleArn.isEmpty(), E_NO_ARN);
    String policyJson = policy.isPresent() ?
        MODEL.toJson(policy.get()) : "";
    final STSClientFactory.STSClient client = prepareSTSClient()
        .orElseThrow(() -> {
          // we've come in on a parent binding, so fail fast
          LOG.error("Cannot issue delegation tokens because the credential"
              + " providers listed in " + DELEGATION_TOKEN_CREDENTIALS_PROVIDER
              + " are returning session tokens");
          return new DelegationTokenIOException(
              E_NO_SESSION_TOKENS_FOR_ROLE_BINDING);
        });
    Credentials credentials = client
        .requestRole(roleArn,
            UUID.randomUUID().toString(),
            policyJson,
            getDuration(),
            TimeUnit.SECONDS);
    return new RoleTokenIdentifier(
        getCanonicalUri(),
        getOwnerText(),
        renewer,
        fromSTSCredentials(credentials),
        encryptionSecrets,
        AbstractS3ATokenIdentifier.createDefaultOriginMessage()
            + " Role ARN=" + roleArn);
  }

  @Override
  public RoleTokenIdentifier createEmptyIdentifier() {
    return new RoleTokenIdentifier();
  }

  @Override
  public String getDescription() {
    return super.getDescription() + " Role ARN=" +
        (roleArn.isEmpty() ? "(none)" : ('"' +  roleArn +'"'));
  }

  @Override
  protected String bindingName() {
    return "Role";
  }
}
