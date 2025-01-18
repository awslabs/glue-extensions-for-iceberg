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

package software.amazon.glue.s3a.impl;

import static software.amazon.glue.s3a.impl.InternalConstants.SC_404;

import com.amazonaws.AmazonServiceException;

/**
 * Translate from AWS SDK-wrapped exceptions into IOExceptions with
 * as much information as possible.
 * The core of the translation logic is in S3AUtils, in
 * {@code translateException} and nearby; that has grown to be
 * a large a complex piece of logic, as it ties in with retry/recovery
 * policies, throttling, etc.
 *
 * This class is where future expansion of that code should go so that we have
 * an isolated place for all the changes..
 * The existing code las been left in S3AUtils it is to avoid cherry-picking
 * problems on backports.
 */
public class ErrorTranslation {

  /**
   * Private constructor for utility class.
   */
  private ErrorTranslation() {
  }

  /**
   * Does this exception indicate that the AWS Bucket was unknown.
   * @param e exception.
   * @return true if the status code and error code mean that the
   * remote bucket is unknown.
   */
  public static boolean isUnknownBucket(AmazonServiceException e) {
    return e.getStatusCode() == SC_404
        && AwsErrorCodes.E_NO_SUCH_BUCKET.equals(e.getErrorCode());
  }

  /**
   * Does this exception indicate that a reference to an object
   * returned a 404. Unknown bucket errors do not match this
   * predicate.
   * @param e exception.
   * @return true if the status code and error code mean that the
   * HEAD request returned 404 but the bucket was there.
   */
  public static boolean isObjectNotFound(AmazonServiceException e) {
    return e.getStatusCode() == SC_404 && !isUnknownBucket(e);
  }

  /**
   * AWS error codes explicitly recognized and processes specially;
   * kept in their own class for isolation.
   */
  public static final class AwsErrorCodes {

    /**
     * The AWS S3 error code used to recognize when a 404 means the bucket is
     * unknown.
     */
    public static final String E_NO_SUCH_BUCKET = "NoSuchBucket";

    /** private constructor. */
    private AwsErrorCodes() {
    }
  }
}
