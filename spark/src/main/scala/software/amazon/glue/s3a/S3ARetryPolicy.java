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

import static software.amazon.glue.s3a.Constants.*;
import static org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry;
import static org.apache.hadoop.io.retry.RetryPolicies.retryByException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.AWSBadRequestException;
import software.amazon.glue.s3a.AWSClientIOException;
import software.amazon.glue.s3a.AWSNoResponseException;
import software.amazon.glue.s3a.AWSRedirectException;
import software.amazon.glue.s3a.AWSS3IOException;
import software.amazon.glue.s3a.AWSServiceIOException;
import software.amazon.glue.s3a.AWSServiceThrottledException;
import software.amazon.glue.s3a.AWSStatus500Exception;
import software.amazon.glue.s3a.MetadataPersistenceException;
import software.amazon.glue.s3a.NoVersionAttributeException;
import software.amazon.glue.s3a.RemoteFileChangedException;
import software.amazon.glue.s3a.S3AUtils;
import software.amazon.glue.s3a.UnknownStoreException;
import software.amazon.glue.s3a.auth.NoAuthWithAWSException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The S3A request retry policy.
 *
 * This uses the retry options in the configuration file to determine retry
 * count and delays for "normal" retries and separately, for throttling;
 * the latter is best handled for longer with an exponential back-off.
 *
 * <ol>
 * <li> Those exceptions considered unrecoverable (networking) are
 *    failed fast.</li>
 * <li>All non-IOEs are failed immediately. Assumed: bugs in code,
 *    unrecoverable errors, etc</li>
 * </ol>
 *
 * For non-idempotent operations, only failures due to throttling or
 * from failures which are known to only arise prior to talking to S3
 * are retried.
 *
 * The retry policy is all built around that of the normal IO exceptions,
 * particularly those extracted from
 * {@link S3AUtils#translateException(String, Path, AmazonClientException)}.
 * Because the {@link #shouldRetry(Exception, int, int, boolean)} method
 * does this translation if an {@code AmazonClientException} is processed,
 * the policy defined for the IOEs also applies to the original exceptions.
 *
 * Put differently: this retry policy aims to work for handlers of the
 * untranslated exceptions, as well as the translated ones.
 * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">S3 Error responses</a>
 * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/ErrorBestPractices.html">Amazon S3 Error Best Practices</a>
 * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html">Dynamo DB Commmon errors</a>
 */
@SuppressWarnings("visibilitymodifier")  // I want a struct of finals, for real.
public class S3ARetryPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ARetryPolicy.class);

  private final Configuration configuration;

  /** Final retry policy we end up with. */
  private final RetryPolicy retryPolicy;

  // Retry policies for mapping exceptions to

  /** Exponential policy for the base of normal failures. */
  protected final RetryPolicy baseExponentialRetry;

  /** Idempotent calls which raise IOEs are retried.
   *  */
  protected final RetryPolicy retryIdempotentCalls;

  /** Policy for throttle requests, which are considered repeatable, even for
   * non-idempotent calls, as the service rejected the call entirely. */
  protected final RetryPolicy throttlePolicy;

  /** No retry on network and tangible API issues. */
  protected final RetryPolicy fail = RetryPolicies.TRY_ONCE_THEN_FAIL;

  /**
   * Client connectivity: baseExponentialRetry without worrying about whether
   * or not the command is idempotent.
   */
  protected final RetryPolicy connectivityFailure;

  /**
   * Instantiate.
   * @param conf configuration to read.
   */
  public S3ARetryPolicy(Configuration conf) {
    Preconditions.checkArgument(conf != null, "Null configuration");
    this.configuration = conf;

    // base policy from configuration
    int limit = conf.getInt(RETRY_LIMIT, RETRY_LIMIT_DEFAULT);
    long interval = conf.getTimeDuration(RETRY_INTERVAL,
        RETRY_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    baseExponentialRetry = exponentialBackoffRetry(
        limit,
        interval,
        TimeUnit.MILLISECONDS);

    LOG.debug("Retrying on recoverable AWS failures {} times with an"
        + " initial interval of {}ms", limit, interval);

    // which is wrapped by a rejection of failures of non-idempotent calls
    // except for specific exceptions considered recoverable.
    // idempotent calls are retried on IOEs but not other exceptions
    retryIdempotentCalls = new FailNonIOEs(
        new IdempotencyRetryFilter(baseExponentialRetry));

    // and a separate policy for throttle requests, which are considered
    // repeatable, even for non-idempotent calls, as the service
    // rejected the call entirely
    throttlePolicy = createThrottleRetryPolicy(conf);

    // client connectivity: fixed retries without care for idempotency
    connectivityFailure = baseExponentialRetry;

    Map<Class<? extends Exception>, RetryPolicy> policyMap =
        createExceptionMap();
    retryPolicy = retryByException(retryIdempotentCalls, policyMap);
  }

  /**
   * Create the throttling policy.
   * This will be called from the S3ARetryPolicy constructor, so
   * subclasses must assume they are not initialized.
   * @param conf configuration to use.
   * @return the retry policy for throttling events.
   */
  protected RetryPolicy createThrottleRetryPolicy(final Configuration conf) {
    return exponentialBackoffRetry(
        conf.getInt(RETRY_THROTTLE_LIMIT, RETRY_THROTTLE_LIMIT_DEFAULT),
        conf.getTimeDuration(RETRY_THROTTLE_INTERVAL,
            RETRY_THROTTLE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  /**
   * Subclasses can override this like a constructor to change behavior: call
   * superclass method, then modify it as needed, and return it.
   * @return Map from exception type to RetryPolicy
   */
  protected Map<Class<? extends Exception>, RetryPolicy> createExceptionMap() {
    // the policy map maps the exact classname; subclasses do not
    // inherit policies.
    Map<Class<? extends Exception>, RetryPolicy> policyMap = new HashMap<>();

    // failfast exceptions which we consider unrecoverable
    policyMap.put(UnknownHostException.class, fail);
    policyMap.put(NoRouteToHostException.class, fail);
    policyMap.put(InterruptedException.class, fail);
    // note this does not pick up subclasses (like socket timeout)
    policyMap.put(InterruptedIOException.class, fail);
    // Access denial and auth exceptions are not retried
    policyMap.put(AccessDeniedException.class, fail);
    policyMap.put(NoAuthWithAWSException.class, fail);
    policyMap.put(FileNotFoundException.class, fail);
    policyMap.put(UnknownStoreException.class, fail);
    policyMap.put(InvalidRequestException.class, fail);

    // metadata stores should do retries internally when it makes sense
    // so there is no point doing another layer of retries after that
    policyMap.put(MetadataPersistenceException.class, fail);

    // once the file has changed, trying again is not going to help
    policyMap.put(RemoteFileChangedException.class, fail);

    // likely only recovered by changing the policy configuration or s3
    // implementation
    policyMap.put(NoVersionAttributeException.class, fail);

    // should really be handled by resubmitting to new location;
    // that's beyond the scope of this retry policy
    policyMap.put(AWSRedirectException.class, fail);

    // throttled requests are can be retried, always
    policyMap.put(AWSServiceThrottledException.class, throttlePolicy);

    // connectivity problems are retried without worrying about idempotency
    policyMap.put(ConnectTimeoutException.class, connectivityFailure);

    // this can be a sign of an HTTP connection breaking early.
    // which can be reacted to by another attempt if the request was idempotent.
    // But: could also be a sign of trying to read past the EOF on a GET,
    // which isn't going to be recovered from
    policyMap.put(EOFException.class, retryIdempotentCalls);

    // policy on a 400/bad request still ambiguous.
    // Treated as an immediate failure
    policyMap.put(AWSBadRequestException.class, fail);

    // Status 500 error code is also treated as a connectivity problem
    policyMap.put(AWSStatus500Exception.class, connectivityFailure);

    // server didn't respond.
    policyMap.put(AWSNoResponseException.class, retryIdempotentCalls);

    // other operations
    policyMap.put(AWSClientIOException.class, retryIdempotentCalls);
    policyMap.put(AWSServiceIOException.class, retryIdempotentCalls);
    policyMap.put(AWSS3IOException.class, retryIdempotentCalls);
    policyMap.put(SocketTimeoutException.class, retryIdempotentCalls);

    // Dynamo DB exceptions
    // asking for more than you should get. It's a retry but should be logged
    // trigger sleep
    policyMap.put(ProvisionedThroughputExceededException.class, throttlePolicy);

    return policyMap;
  }

  @Override
  public RetryAction shouldRetry(Exception exception,
      int retries,
      int failovers,
      boolean idempotent) throws Exception {
    Preconditions.checkArgument(exception != null, "Null exception");
    Exception ex = exception;
    if (exception instanceof AmazonClientException) {
      // uprate the amazon client exception for the purpose of exception
      // processing.
      ex = S3AUtils.translateException("", "",
          (AmazonClientException) exception);
    }
    return retryPolicy.shouldRetry(ex, retries, failovers, idempotent);
  }

  /**
   * Get the configuration this policy was created from.
   * @return the configuration.
   */
  protected Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Policy which fails fast any non-idempotent call; hands off
   * all idempotent calls to the next retry policy.
   */
  private static final class IdempotencyRetryFilter implements RetryPolicy {

    private final RetryPolicy next;

    IdempotencyRetryFilter(RetryPolicy next) {
      this.next = next;
    }

    @Override
    public RetryAction shouldRetry(Exception e,
        int retries,
        int failovers,
        boolean idempotent) throws Exception {
      return
          idempotent ?
              next.shouldRetry(e, retries, failovers, true)
              : RetryAction.FAIL;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "IdempotencyRetryFilter{");
      sb.append("next=").append(next);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * All non-IOE exceptions are failed.
   */
  private static final class FailNonIOEs implements RetryPolicy {

    private final RetryPolicy next;

    private FailNonIOEs(RetryPolicy next) {
      this.next = next;
    }

    @Override
    public RetryAction shouldRetry(Exception e,
        int retries,
        int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return
          e instanceof IOException ?
              next.shouldRetry(e, retries, failovers, true)
              : RetryAction.FAIL;
    }
  }

}
