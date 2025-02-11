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

package software.amazon.glue.s3a.util;

import static software.amazon.glue.s3a.Constants.FS_S3A_CREDENTIALS_RESOLVER;
import static software.amazon.glue.s3a.Constants.FS_S3A_CREDENTIALS_RESOLVER_DEFAULT;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.identity.FileSystemOwner;
import software.amazon.glue.s3a.resolver.S3Call;
import software.amazon.glue.s3a.resolver.S3CredentialsResolver;
import software.amazon.glue.s3a.resolver.S3Request;
import software.amazon.glue.s3a.resolver.S3RequestCall;
import software.amazon.glue.s3a.resolver.S3Resource;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CredentialsResolverUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3CredentialsResolverUtils.class);

  /**
   * Initialise implementation of {@link S3CredentialsResolver}
   *
   * @param conf            Hadoop Configuration
   * @param fileSystemOwner File System Owner
   * @return {@link S3CredentialsResolver}
   */
  public static S3CredentialsResolver initS3CredentialsResolver(
      Configuration conf,
      FileSystemOwner fileSystemOwner) {

    Optional<String> resolverClass = s3CredentialsResolverClass(conf);
    return resolverClass.map(clazz -> newS3CredentialsResolver(
        clazz,
        conf,
        fileSystemOwner
    )).orElse(null);
  }

  /**
   * Check if {@link S3CredentialsResolver} is enabled
   *
   * @param conf Hadoop Configuration
   * @return {@link S3CredentialsResolver} is enabled
   */
  public static boolean isS3CredentialsResolverEnabled(Configuration conf) {
    return s3CredentialsResolverClass(conf).isPresent();
  }

  /**
   * Get Credentials Resolver Class from config
   *
   * @param conf Hadoop Configuration
   * @return Credentials Resolver Class
   */
  public static Optional<String> s3CredentialsResolverClass(Configuration conf) {
    String className = conf.get(FS_S3A_CREDENTIALS_RESOLVER, FS_S3A_CREDENTIALS_RESOLVER_DEFAULT);

    if (FS_S3A_CREDENTIALS_RESOLVER_DEFAULT.equals(className)) {
      return Optional.empty();
    } else {
      return Optional.of(className);
    }
  }

  /**
   * Get instance of {@link S3CredentialsResolver}
   *
   * @param fullyQualifiedClassName Fully Qualified Class Name
   * @param conf                    Hadoop configuration
   * @param fileSystemOwner         File System Owner
   * @return instance of {@link S3CredentialsResolver}
   */
  public static S3CredentialsResolver newS3CredentialsResolver(
      String fullyQualifiedClassName,
      Configuration conf,
      FileSystemOwner fileSystemOwner) {
    LOG.info("Loading credentials resolver class: {}", fullyQualifiedClassName);
    try {
      Class clazz = Class.forName(fullyQualifiedClassName);
      Constructor<S3CredentialsResolver> ctor = clazz.getDeclaredConstructor(
          Configuration.class,
          FileSystemOwner.class);
      return ctor.newInstance(conf, fileSystemOwner);
    } catch (IllegalAccessException e) {
      String msg = String.format("Unable to access constructor of class %s. Message: %s", fullyQualifiedClassName,
          e.getMessage());
      LOG.error(msg);
    } catch (IllegalArgumentException | NoSuchMethodException e) {
      String msg = String.format(
          "Constructor of %s does not match with specification. Expected (%s, %s). Message: %s",
          fullyQualifiedClassName,
          Configuration.class.getName(),
          FileSystemOwner.class.getName(),
          e.getMessage());
      LOG.error(msg);
    } catch (InstantiationException e) {
      String msg = String.format(
          "%s is abstract class. Only concrete implementations can be instantiated. Message: %s",
          fullyQualifiedClassName,
          e.getMessage());
      LOG.error(msg);
    } catch (InvocationTargetException e) {
      String msg =
          String.format("Constructor for %s threw exception. Message: %s", fullyQualifiedClassName, e.getMessage());
      LOG.error(msg);
    } catch (ExceptionInInitializerError e) {
      String msg =
          String.format("Failure occurred during static initialization of %s. Message: %s", fullyQualifiedClassName,
              e.getMessage());
      LOG.error(msg);
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      String msg = String.format("%s not found. Cause: %s", fullyQualifiedClassName, e.getMessage());
      LOG.error(msg);
    } catch (Exception e) {
      String msg =
          String.format("Unknown failure: unable to create new instance of %s. Message: %s", fullyQualifiedClassName,
              e.getMessage());
      LOG.error(msg);
    }
    return null;
  }

  /**
   * Create {@link S3Call} for the request
   *
   * @param scheme         S3 scheme
   * @param bucket         S3 bucket
   * @param type           {@link S3Resource.Type}
   * @param s3Request      {@link S3Request}
   * @param additionalArgs Optional key, prefix, or list of keys
   * @return {@link S3Call} for the request
   */
  public static S3Call createS3Call(
      String scheme,
      String bucket,
      S3Resource.Type type,
      S3Request s3Request,
      Object... additionalArgs) {
    S3RequestCall s3RequestCall = new S3RequestCall();
    // Set common fields
    s3RequestCall.setBucket(bucket);
    s3RequestCall.setS3Requests(Collections.singletonList(s3Request));

    // Determine the appropriate additionalArgs
    if (additionalArgs.length == 0) {
      // No keys provided
      s3RequestCall.setS3Resources(createS3Resources(type, scheme, bucket));
    } else if (additionalArgs[0] instanceof String) {
      // Key or prefix provided
      String keyOrPrefix = (String) additionalArgs[0];
      s3RequestCall.setS3Resources(createS3Resources(type, scheme, bucket, keyOrPrefix));
    } else if (additionalArgs[0] instanceof List) {
      // List of keys provided
      List<String> keys = (List<String>) additionalArgs[0];
      s3RequestCall.setS3Resources(createS3Resources(type, scheme, bucket, keys));
    } else {
      throw new IllegalArgumentException("Invalid additional arguments");
    }

    return s3RequestCall;
  }

  /**
   * Create {@link FileSystemOwner}
   *
   * @return {@link FileSystemOwner}
   */
  public static FileSystemOwner createFileSystemOwner(UserGroupInformation owner) {
    return new FileSystemOwner() {
      @Override
      public String getFullUserName() {
        return owner.getUserName();
      }

      @Override
      public String getShortUserName() {
        return owner.getShortUserName();
      }

      @Override
      public String getGroup() throws IOException {
        return owner.getPrimaryGroupName();
      }

      @Override
      public String[] getGroupNames() {
        return owner.getGroupNames();
      }
    };
  }

  /**
   * Create a List of {@link S3Resource}.
   *
   * @param type   {@link S3Resource.Type}
   * @param scheme Scheme
   * @param bucket Bucket Name
   * @param keys   Optional keys (can be null or empty for bucket root)
   * @return List of {@link S3Resource}
   */
  public static List<S3Resource> createS3Resources(S3Resource.Type type, String scheme, String bucket, Object... keys) {
    if (keys.length == 0) {
      // No keys provided
      return Collections.singletonList(createS3Resource(type, scheme, bucket, null));
    } else if (keys[0] instanceof String) {
      // Key or prefix provided
      return Collections.singletonList(createS3Resource(type, scheme, bucket, (String) keys[0]));
    } else if (keys[0] instanceof List) {
      // List of keys are provided
      List<String> keyList = (List<String>) keys[0];
      return keyList.stream()
          .map(key -> createS3Resource(type, scheme, bucket, key))
          .collect(Collectors.toList());
    } else {
      throw new IllegalArgumentException("Invalid keys argument");
    }
  }

  /**
   * Create a single {@link S3Resource}.
   *
   * @param type   {@link S3Resource.Type}
   * @param scheme Scheme
   * @param bucket Bucket Name
   * @param key    Key (optional, can be null)
   * @return An {@link S3Resource}
   */
  public static S3Resource createS3Resource(S3Resource.Type type, String scheme, String bucket, String key) {
    return new S3Resource() {
      @Override
      public Type getType() {
        return type;
      }

      @Override
      public String getBucketName() {
        return bucket;
      }

      @Override
      public String getPath() {
        return scheme + "://" + bucket + (key == null ? "/" : "/" + key);
      }
    };
  }
}
