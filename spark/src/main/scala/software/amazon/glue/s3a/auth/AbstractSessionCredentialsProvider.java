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

import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSCredentials;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.CredentialInitializationException;
import software.amazon.glue.s3a.Invoker;
import software.amazon.glue.s3a.Retries;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Base class for session credential support.
 */
@InterfaceAudience.Private
public abstract class AbstractSessionCredentialsProvider
    extends AbstractAWSCredentialProvider {

  /** Credentials, created in {@link #init()}. */
  private AWSCredentials awsCredentials;

  /** Atomic flag for on-demand initialization. */
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  /**
   * The (possibly translated) initialization exception.
   * Used for testing.
   */
  private IOException initializationException;

  /**
   * Constructor.
   * @param uri possibly null filesystem URI.
   * @param conf configuration.
   */
  public AbstractSessionCredentialsProvider(
      @Nullable final URI uri,
      final Configuration conf) {
    super(uri, conf);
  }

  /**
   * Initialize the credentials by calling
   * {@link #createCredentials(Configuration)} with the current config.
   * @throws IOException on any failure.
   */
  @Retries.OnceTranslated
  protected void init() throws IOException {
    // stop re-entrant attempts
    if (initialized.getAndSet(true)) {
      return;
    }
    try {
      awsCredentials = Invoker.once("create credentials", "",
          () -> createCredentials(getConf()));
    } catch (IOException e) {
      initializationException = e;
      throw e;
    }
  }

  /**
   * Has an attempt to initialize the credentials been attempted?
   * @return true if {@code init()} was called.
   */
  public boolean isInitialized() {
    return initialized.get();
  }

  /**
   * Implementation point: whatever the subclass must do to load credentials.
   * This is called from {@link #init()} and then the credentials are cached,
   * along with any exception.
   * @param config the configuration
   * @return the credentials
   * @throws IOException on any failure.
   */
  protected abstract AWSCredentials createCredentials(Configuration config)
      throws IOException;

  /**
   * Get the credentials.
   * Any exception raised in
   * {@link #createCredentials(Configuration)}
   * is thrown here before any attempt to return the credentials
   * is made.
   * @return credentials, if set.
   * @throws SdkBaseException if one was raised during init
   * @throws CredentialInitializationException on other failures.
   */
  public AWSCredentials getCredentials() throws SdkBaseException {
    // do an on-demand init then raise an AWS SDK exception if
    // there was a failure.
    try {
      if (!isInitialized()) {
        init();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof SdkBaseException) {
        throw (SdkBaseException) e.getCause();
      } else {
        throw new CredentialInitializationException(e.getMessage(), e);
      }
    }
    if (awsCredentials == null) {
      throw new CredentialInitializationException(
          "Provider " + this + " has no credentials");
    }
    return awsCredentials;
  }

  public final boolean hasCredentials() {
    return awsCredentials == null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * Get any IOE raised during initialization.
   * Null if {@link #init()} hasn't been called, or it actually worked.
   * @return an exception or null.
   */
  @VisibleForTesting
  public IOException getInitializationException() {
    return initializationException;
  }

  /**
   * A special set of null credentials which are not the anonymous class.
   * This will be interpreted as "this provider has no credentials to offer",
   * rather than an explicit error or anonymous access.
   */
  protected static final class NoCredentials implements AWSCredentials {
    @Override
    public String getAWSAccessKeyId() {
      return null;
    }

    @Override
    public String getAWSSecretKey() {
      return null;
    }
  }
}
