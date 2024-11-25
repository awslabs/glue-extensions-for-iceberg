package software.amazon.glue.s3a.resolver;

import java.util.Collection;
import java.util.List;

public interface S3Call {
  /**
   * Returns the bucket name which this S3 call will use.
   *
   * @return the bucket name
   */
  String getBucketName();

  /**
   * Returns a {@link Collection} of {@link S3Resource}s used by this S3Call.
   *
   * @return the {@link S3Resource}s required
   * @see S3Resource
   */
  Collection<S3Resource> getS3Resources();

  /**
   * Returns a list of {@link S3Request} used by this S3 call.
   * {@link S3Request} contains the request and action name.
   */
  List<S3Request> getS3Requests();
}