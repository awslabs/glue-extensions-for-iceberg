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

import static software.amazon.glue.s3a.impl.NetworkBinding.isAwsEndpoint;

/**
 * Anything needed to support Amazon S3 Express One Zone Storage.
 * These have bucket names like {@code s3a://bucket--usw2-az2--x-s3/}
 */
public final class S3ExpressStorage {

  /**
   * Is this S3Express storage? value {@value}.
   */
  public static final String STORE_CAPABILITY_S3_EXPRESS_STORAGE =
      "fs.s3a.capability.s3express.storage";

  /**
   * What is the official product name? used for error messages and logging: {@value}.
   */
  public static final String PRODUCT_NAME = "Amazon S3 Express One Zone Storage";

  private S3ExpressStorage() {
  }

  /**
   * Minimum length of a region.
   */
  private static final int SUFFIX_LENGTH = "--usw2-az2--x-s3".length();

  public static final int ZONE_LENGTH = "usw2-az2".length();

  /**
   * Suffix of S3Express storage bucket names..
   */
  public static final String S3EXPRESS_STORE_SUFFIX = "--x-s3";

  /**
   * Is a bucket an S3Express store?
   * This may get confused against third party stores, so takes the endpoint
   * and only supports aws endpoints round the world.
   * @param bucket bucket to probe
   * @param endpoint endpoint string. If empty, this is considered an AWS endpoint.
   * @return true if the store is S3 Express.
   */
  public static boolean isS3ExpressStore(String bucket, final String endpoint) {
    return isAwsEndpoint(endpoint) && hasS3ExpressSuffix(bucket);
  }

  /**
   * Check for a bucket name matching -does not look at endpoint.
   * @param bucket bucket to probe.
   * @return true if the suffix is present
   */
  public static boolean hasS3ExpressSuffix(final String bucket) {
    return bucket.endsWith(S3EXPRESS_STORE_SUFFIX);
  }

}
