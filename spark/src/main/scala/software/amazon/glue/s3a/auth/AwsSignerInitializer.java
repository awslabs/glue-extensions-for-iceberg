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

package software.amazon.glue.s3a.auth;

import org.apache.hadoop.conf.Configuration;
import software.amazon.glue.s3a.S3AFileSystem;
import software.amazon.glue.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Interface which can be implemented to allow initialization of any custom
 * signers which may be used by the {@link S3AFileSystem}.
 */
public interface AwsSignerInitializer {

  /**
   * Register a store instance.
   *
   * @param bucketName the bucket name
   * @param storeConf the store configuration
   * @param dtProvider delegation token provider for the store
   * @param storeUgi ugi under which the store is operating
   */
  void registerStore(String bucketName, Configuration storeConf,
      DelegationTokenProvider dtProvider, UserGroupInformation storeUgi);

  /**
   * Unregister a store instance.
   *
   * @param bucketName the bucket name
   * @param storeConf the store configuration
   * @param dtProvider delegation token provider for the store
   * @param storeUgi ugi under which the store is operating
   */
  void unregisterStore(String bucketName, Configuration storeConf,
      DelegationTokenProvider dtProvider, UserGroupInformation storeUgi);
}
