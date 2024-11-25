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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.S3AEncryptionMethods;
import software.amazon.glue.s3a.S3AInputStream;

/**
 * This class holds attributes of an object independent of the
 * file status type.
 * It is used in {@link S3AInputStream} and elsewhere.
 * as a way to reduce parameters being passed
 * to the constructor of such class,
 * and elsewhere to be a source-neutral representation of a file status.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3ObjectAttributes {
  private final String bucket;
  private final Path path;
  private final String key;
  private final S3AEncryptionMethods serverSideEncryptionAlgorithm;
  private final String serverSideEncryptionKey;
  private final String eTag;
  private final String versionId;
  private final long len;

  /**
   * Constructor.
   * @param bucket s3 bucket
   * @param path path
   * @param key object key
   * @param serverSideEncryptionAlgorithm current encryption algorithm
   * @param serverSideEncryptionKey any server side encryption key?
   * @param len object length
   * @param eTag optional etag
   * @param versionId optional version id
   */
  public S3ObjectAttributes(
      String bucket,
      Path path,
      String key,
      S3AEncryptionMethods serverSideEncryptionAlgorithm,
      String serverSideEncryptionKey,
      String eTag,
      String versionId,
      long len) {
    this.bucket = bucket;
    this.path = path;
    this.key = key;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;
    this.serverSideEncryptionKey = serverSideEncryptionKey;
    this.eTag = eTag;
    this.versionId = versionId;
    this.len = len;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
    return serverSideEncryptionAlgorithm;
  }

  public String getServerSideEncryptionKey() {
    return serverSideEncryptionKey;
  }

  public String getETag() {
    return eTag;
  }

  public String getVersionId() {
    return versionId;
  }

  public long getLen() {
    return len;
  }

  public Path getPath() {
    return path;
  }
}
