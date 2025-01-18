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

package software.amazon.glue.s3a.resolver;

/**
 * Represents some S3 resource.
 *
 * <p>
 * The intention of this interface is to abstract away any concept of a "thing" you may need to
 * interact with in S3.
 * </p>
 */
public interface S3Resource {

  /**
   * Returns the {@link Type} of this S3Resource.
   *
   * @return the Type
   */
  Type getType();

  /**
   * Returns the name of the S3 bucket for this particular S3Resource.
   * <p>
   * Every S3Resource must necessarily have a bucket.
   * </p>
   *
   * @return the bucket name
   * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html">
   * Bucket naming restrictions</a>
   */
  String getBucketName();

  /**
   * Returns the path for this particular S3Resource.
   * <p>
   * Every S3Resource must necessarily have a path, as demonstrated in above
   * documentation for {@link Type}.
   * </p>
   *
   * @return the Path for this S3Resource
   */
  String getPath();

  /**
   * The Type of this S3 Resource.
   *
   * <p>
   * This type must be one of three separate types:
   * <ul>
   * <li>{@link #BUCKET}</li>
   * <li>{@link #OBJECT}</li>
   * <li>{@link #PREFIX}</li>
   * </ul>
   * </p>
   * <p>
   * For the documentation for each individual Type below, assume an S3 bucket with only one object:
   * s3://myBucket/prefixPart/object
   * </p>
   */
  enum Type {
    /**
     * If this S3Resource is a BUCKET, the path only refers to some S3 bucket itself.
     * <p>
     * {@literal e.g. getBucketName() => myBucket ; getPath() => s3://myBucket}
     * </p>
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#BasicsBucket">
     * Introduction to S3 Buckets</a>
     */
    BUCKET,

    /**
     * If this S3Resource is an OBJECT, it refers to an actual S3 object that can be downloaded,
     * interacted with, etc.
     * <p>
     * {@literal e.g. getBucketName() => myBucket ; getPath() => s3://myBucket/prefixPart/object}
     * </p>
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#BasicsObjects">
     * Introduction to S3 Objects</a>
     */
    OBJECT,

    /**
     * If this S3Resource is a PREFIX, it refers to a <i>part</i> of an S3 path that doesn't
     * point to an
     * object, but could point to any number of objects. Can be thought of as a "directory".
     * <p>
     * {@literal e.g. getBucketName() => myBucket ; getPath() => s3://myBucket/prefixPart}
     * </p>
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html">Example
     * of prefixes in action 1</a>
     * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/UG/searching-for-objects-by-prefix.html">
     * Example of prefixes in action 2</a>
     */
    PREFIX
  }
}
