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

package software.amazon.glue.s3a.s3guard;

import static software.amazon.glue.s3a.Constants.*;
import static org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.S3ARetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * A Retry policy whose throttling comes from the S3Guard config options.
 */
public class S3GuardDataAccessRetryPolicy extends S3ARetryPolicy {

  public S3GuardDataAccessRetryPolicy(final Configuration conf) {
    super(conf);
  }

  protected RetryPolicy createThrottleRetryPolicy(final Configuration conf) {
    return exponentialBackoffRetry(
        conf.getInt(S3GUARD_DDB_MAX_RETRIES, S3GUARD_DDB_MAX_RETRIES_DEFAULT),
        conf.getTimeDuration(S3GUARD_DDB_THROTTLE_RETRY_INTERVAL,
            S3GUARD_DDB_THROTTLE_RETRY_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }
}
