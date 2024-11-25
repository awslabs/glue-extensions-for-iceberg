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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import software.amazon.glue.s3a.resolver.S3Request;
import software.amazon.glue.s3a.resolver.S3CredentialsResolver;
import software.amazon.glue.s3a.resolver.S3Resource;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequest;

import static software.amazon.glue.s3a.Constants.FS_S3A;
import static software.amazon.glue.s3a.Constants.FS_S3A_CREDENTIALS_RESOLVER;
import static software.amazon.glue.s3a.Constants.FS_S3A_CREDENTIALS_RESOLVER_DEFAULT;

public class S3CredentialsProviderUtils {

  private static final Logger LOG = LoggerFactory.getLogger(S3CredentialsProviderUtils.class);

  public static S3CredentialsResolver getS3CredentialsResolver(
      Configuration configuration,
      UserGroupInformation userGroupInformation) {

    Optional<String> resolverClass = getS3CredentialsResolverClass(configuration);
    return resolverClass.map(clazz -> newS3CredentialsResolver(
        clazz,
        configuration,
        userGroupInformation
    )).orElse(null);
  }

  public static Optional<String> getS3CredentialsResolverClass(Configuration conf) {
    String className = conf.get(FS_S3A_CREDENTIALS_RESOLVER, FS_S3A_CREDENTIALS_RESOLVER_DEFAULT);

    if (className.equals(FS_S3A_CREDENTIALS_RESOLVER_DEFAULT)) {
      return Optional.empty();
    } else {
      return Optional.of(className);
    }
  }

  public static S3CredentialsResolver newS3CredentialsResolver(
      String fullyQualifiedClassName,
      Configuration hadoopConfiguration,
      UserGroupInformation userGroupInformation) {
    LOG.info("Loading credentials resolver class: {}", fullyQualifiedClassName);
    try {
      Class clazz = Class.forName(fullyQualifiedClassName);
      Constructor<S3CredentialsResolver> ctor = clazz.getDeclaredConstructor(
          Configuration.class,
          UserGroupInformation.class);
      return ctor.newInstance(hadoopConfiguration, userGroupInformation);
    } catch (IllegalAccessException e) {
      String msg = "Unable to access constructor of class " + fullyQualifiedClassName;
      LOG.error(msg);
    } catch (IllegalArgumentException | NoSuchMethodException e) {
      String msg = String.format(
          "Constructor of %s does not match with specification. expected (%s, %s)",
          fullyQualifiedClassName,
          Configuration.class.getName(),
          UserGroupInformation.class.getName());
      LOG.error(msg);
    } catch (InstantiationException e) {
      String msg = String.format(
          "%s is abstract class. Only concrete implementations can be instantiated",
          fullyQualifiedClassName);
      LOG.error(msg);
    } catch (InvocationTargetException e) {
      String msg = String.format(
          "Constructor for %s threw exception, Cause: ",
          fullyQualifiedClassName);
      LOG.error(msg);
    } catch (ExceptionInInitializerError e) {
      String msg = "Failure occurred during static initialization of " + fullyQualifiedClassName;
      LOG.error(msg);
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      String msg = fullyQualifiedClassName + " not found";
      LOG.error(msg);
    } catch (Exception e) {
      String msg = "Unknown failure: unable to create new instance of " + fullyQualifiedClassName;
      LOG.error(msg);
    }
    return null;
  }

  public static <T extends AwsRequest.Builder> S3Request getS3Request(T t) {
    String s3RequestName = t.build().getClass().getSimpleName();
    LOG.debug("S3 Request: {}", s3RequestName);
    try {
      return S3Request.valueOf(s3RequestName);
    } catch (IllegalArgumentException e) {
      LOG.error("Could not find S3Request for {}", s3RequestName);
    }
    return null;
  }

  public static List<S3Resource> getS3Resources(S3Resource.Type type, String bucket, List<String> keys) {
    List<S3Resource> s3Resources = new ArrayList<>();
    for (String key : keys) {
      s3Resources.add(getS3Resource(type, bucket, key));
    }
    return s3Resources;
  }

  public static List<S3Resource> getS3Resources(S3Resource.Type type, String bucket, String key) {
    return Collections.singletonList(getS3Resource(type, bucket, key));
  }

  public static List<S3Resource> getS3Resources(S3Resource.Type type, String bucket) {
    return Collections.singletonList(getS3Resource(type, bucket, null));
  }

  public static S3Resource getS3Resource(S3Resource.Type type, String bucket, String key) {
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
        if (key == null) {
          return FS_S3A + "://" + bucket + "/";
        } else {
          return FS_S3A + "://" + bucket + "/" + key;
        }
      }
    };
  }
}