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

import static software.amazon.glue.s3a.impl.InternalConstants.DEFAULT_UPLOAD_PART_COUNT_LIMIT;
import static software.amazon.glue.s3a.util.S3CredentialsResolverUtils.createS3Call;
import static software.amazon.glue.s3a.util.S3CredentialsResolverUtils.initS3CredentialsResolver;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIOException;
import software.amazon.glue.s3a.api.RequestFactory;
import software.amazon.glue.s3a.auth.delegation.EncryptionSecrets;
import software.amazon.glue.s3a.identity.FileSystemOwner;
import software.amazon.glue.s3a.resolver.S3Call;
import software.amazon.glue.s3a.resolver.S3CredentialsResolver;
import software.amazon.glue.s3a.resolver.S3Request;
import software.amazon.glue.s3a.resolver.S3Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The standard implementation of the request factory. This creates AWS SDK request classes for the
 * specific bucket, with standard options/headers set. It is also where custom setting parameters
 * can take place.
 *
 * <p>All created request builders will be passed to {@link
 * PrepareRequest#prepareRequest(AmazonWebServiceRequest)} before being returned to the caller.
 */
public class CredentialsResolverRequestFactoryImpl extends RequestFactoryImpl {

  public static final Logger LOG =
      LoggerFactory.getLogger(CredentialsResolverRequestFactoryImpl.class);

  private final Configuration conf;

  private final FileSystemOwner fileSystemOwner;

  private final String scheme;

  /**
   * Constructor.
   *
   * @param requestFactoryBuilder requestFactoryBuilder with all the configuration.
   * @param credentialsResolverRequestFactoryBuilder {@link
   *     CredentialsResolverRequestFactoryBuilder} with all configurations
   */
  protected CredentialsResolverRequestFactoryImpl(
      final RequestFactoryBuilder requestFactoryBuilder,
      CredentialsResolverRequestFactoryBuilder credentialsResolverRequestFactoryBuilder) {
    super(requestFactoryBuilder);
    this.conf = credentialsResolverRequestFactoryBuilder.conf;
    this.fileSystemOwner = credentialsResolverRequestFactoryBuilder.fileSystemOwner;
    this.scheme = credentialsResolverRequestFactoryBuilder.scheme;
  }

  /**
   * Add the {@link S3CredentialsResolver} if the request is of {@link AmazonWebServiceRequest} type
   *
   * @param t request
   * @param s3Call {@link S3Call}
   * @param <T> request
   */
  private <T extends AmazonWebServiceRequest> void maybeAddRequestCredentialsProvider(
      T t, S3Call s3Call) {
    S3CredentialsResolver s3CredentialsResolver = initS3CredentialsResolver(conf, fileSystemOwner);
    if (s3CredentialsResolver != null) {
      LOG.debug("S3Call: {}", s3Call);
      AWSCredentialsProvider provider = s3CredentialsResolver.resolve(s3Call);
      if (provider != null) {
        t.setRequestCredentialsProvider(provider);
        LOG.debug("Credentials provider is set at request: {}", t);
      } else {
        LOG.warn("Credentials provider is not set at request: {}", t);
      }
    }
  }

  @Override
  public CopyObjectRequest newCopyObjectRequest(
      String srcKey, String dstKey, ObjectMetadata srcom) {
    CopyObjectRequest request = super.newCopyObjectRequest(srcKey, dstKey, srcom);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.CopyObjectRequest, srcKey);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public PutObjectRequest newPutObjectRequest(
      String key, final ObjectMetadata metadata, File srcfile) {
    PutObjectRequest request = super.newPutObjectRequest(key, metadata, srcfile);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.PutObjectRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public PutObjectRequest newPutObjectRequest(
      String key, ObjectMetadata metadata, InputStream inputStream) {
    PutObjectRequest request = super.newPutObjectRequest(key, metadata, inputStream);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.PutObjectRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public PutObjectRequest newDirectoryMarkerRequest(String directory) {
    PutObjectRequest request = super.newDirectoryMarkerRequest(directory);
    String key = directory.endsWith("/") ? directory : (directory + "/");
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.PutObjectRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public ListMultipartUploadsRequest newListMultipartUploadsRequest(String prefix) {
    ListMultipartUploadsRequest request = super.newListMultipartUploadsRequest(prefix);
    S3Call s3Call =
        createS3Call(
            getScheme(),
            getBucket(),
            S3Resource.Type.PREFIX,
            S3Request.ListMultipartUploadsRequest,
            prefix);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public AbortMultipartUploadRequest newAbortMultipartUploadRequest(
      String destKey, String uploadId) {
    AbortMultipartUploadRequest request = super.newAbortMultipartUploadRequest(destKey, uploadId);
    S3Call s3Call =
        createS3Call(
            getScheme(),
            getBucket(),
            S3Resource.Type.OBJECT,
            S3Request.AbortMultipartUploadRequest,
            destKey);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public InitiateMultipartUploadRequest newMultipartUploadRequest(final String destKey) {
    InitiateMultipartUploadRequest request = super.newMultipartUploadRequest(destKey);
    S3Call s3Call =
        createS3Call(
            getScheme(),
            getBucket(),
            S3Resource.Type.OBJECT,
            S3Request.CreateMultipartUploadRequest,
            destKey);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public CompleteMultipartUploadRequest newCompleteMultipartUploadRequest(
      String destKey, String uploadId, List<PartETag> partETags) {
    CompleteMultipartUploadRequest request =
        super.newCompleteMultipartUploadRequest(destKey, uploadId, partETags);
    S3Call s3Call =
        createS3Call(
            getScheme(),
            getBucket(),
            S3Resource.Type.OBJECT,
            S3Request.CompleteMultipartUploadRequest,
            destKey);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public GetObjectMetadataRequest newGetObjectMetadataRequest(String key) {
    GetObjectMetadataRequest request = super.newGetObjectMetadataRequest(key);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.GetObjectMetadataRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public GetObjectRequest newGetObjectRequest(String key) {
    GetObjectRequest request = super.newGetObjectRequest(key);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.GetObjectRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      int size,
      InputStream uploadStream,
      File sourceFile,
      long offset)
      throws PathIOException {
    UploadPartRequest request =
        super.newUploadPartRequest(
            destKey, uploadId, partNumber, size, uploadStream, sourceFile, offset);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.UploadPartRequest, destKey);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public ListObjectsRequest newListObjectsV1Request(
      final String key, final String delimiter, final int maxKeys) {
    ListObjectsRequest request = super.newListObjectsV1Request(key, delimiter, maxKeys);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.PREFIX, S3Request.ListObjectsRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public ListObjectsV2Request newListObjectsV2Request(
      final String key, final String delimiter, final int maxKeys) {
    ListObjectsV2Request request = super.newListObjectsV2Request(key, delimiter, maxKeys);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.PREFIX, S3Request.ListObjectsV2Request, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public DeleteObjectRequest newDeleteObjectRequest(String key) {
    DeleteObjectRequest request = super.newDeleteObjectRequest(key);
    S3Call s3Call =
        createS3Call(
            getScheme(), getBucket(), S3Resource.Type.OBJECT, S3Request.DeleteObjectRequest, key);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  @Override
  public DeleteObjectsRequest newBulkDeleteRequest(
      List<DeleteObjectsRequest.KeyVersion> keysToDelete, boolean quiet) {
    DeleteObjectsRequest request = super.newBulkDeleteRequest(keysToDelete, quiet);
    S3Call s3Call =
        createS3Call(
            getScheme(),
            getBucket(),
            S3Resource.Type.OBJECT,
            S3Request.DeleteObjectRequest,
            keysToDelete);
    maybeAddRequestCredentialsProvider(request, s3Call);
    return request;
  }

  /**
   * Get the scheme
   *
   * @return the scheme.
   */
  protected String getScheme() {
    return scheme;
  }

  /**
   * Create a builder.
   *
   * @return new builder.
   */
  public static CredentialsResolverRequestFactoryBuilder getBuilder() {
    return new CredentialsResolverRequestFactoryBuilder();
  }

  /** Builder. */
  public static final class CredentialsResolverRequestFactoryBuilder {

    /** Target bucket. */
    private String bucket;

    /** Encryption secrets. */
    private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();

    /** ACL For new objects. */
    private CannedAccessControlList cannedACL = null;

    /** Multipart limit. */
    private long multipartPartCountLimit = DEFAULT_UPLOAD_PART_COUNT_LIMIT;

    /** Callback to prepare requests. */
    private PrepareRequest requestPreparer;

    /** Hadoop Configuration */
    private Configuration conf;

    /** File System Owner */
    private FileSystemOwner fileSystemOwner;

    /** Scheme */
    private String scheme;

    private CredentialsResolverRequestFactoryBuilder() {}

    /**
     * Build the request factory.
     *
     * @return the factory
     */
    public RequestFactory build() {
      RequestFactoryBuilder requestFactoryBuilder =
          RequestFactoryImpl.builder()
              .withBucket(bucket)
              .withCannedACL(cannedACL)
              .withEncryptionSecrets(encryptionSecrets)
              .withMultipartPartCountLimit(multipartPartCountLimit)
              .withRequestPreparer(requestPreparer);
      return new CredentialsResolverRequestFactoryImpl(requestFactoryBuilder, this);
    }

    /**
     * Target bucket.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withBucket(final String value) {
      bucket = value;
      return this;
    }

    /**
     * Encryption secrets.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withEncryptionSecrets(
        final EncryptionSecrets value) {
      encryptionSecrets = value;
      return this;
    }

    /**
     * ACL For new objects.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withCannedACL(
        final CannedAccessControlList value) {
      cannedACL = value;
      return this;
    }

    /**
     * Multipart limit.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withMultipartPartCountLimit(final long value) {
      multipartPartCountLimit = value;
      return this;
    }

    /**
     * Callback to prepare requests.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withRequestPreparer(
        final PrepareRequest value) {
      this.requestPreparer = value;
      return this;
    }

    /**
     * Hadoop Configuration.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withConf(final Configuration value) {
      this.conf = value;
      return this;
    }

    /**
     * Hadoop Configuration.
     *
     * @param value new value
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withFileSystemOwner(
        final FileSystemOwner value) {
      this.fileSystemOwner = value;
      return this;
    }

    /**
     * Scheme.
     *
     * @param scheme Scheme of the file system
     * @return the builder
     */
    public CredentialsResolverRequestFactoryBuilder withScheme(final String scheme) {
      this.scheme = scheme;
      return this;
    }
  }
}
