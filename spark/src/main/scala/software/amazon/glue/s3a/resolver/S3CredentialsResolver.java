package software.amazon.glue.s3a.resolver;

import com.amazonaws.auth.AWSCredentialsProvider;
import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * This interface returns an {@link AWSCredentialsProvider} given {@link S3Call}s.
 * <p>
 * This interface is intended to be implemented by users of this SDK to conform to their specific
 * use case. Implementation detail is entirely left to the user.
 * </p>
 *
 * @see AWSCredentialsProvider
 */
public interface S3CredentialsResolver {

  /**
   * Given a {@link S3Call}, returns an {@link AWSCredentialsProvider}
   * to be used for accessing those resources.
   * <p>
   * This function can return null. If null is returned, the default credentials provider will be
   * used.
   * </p>
   *
   * @param s3Call the actual S3 operation {@link S3Call} being performed
   * @return the {@link AWSCredentialsProvider} to use for this context, or null if there is none
   */
  @Nullable
  AwsCredentialsProvider resolve(S3Call s3Call);
}