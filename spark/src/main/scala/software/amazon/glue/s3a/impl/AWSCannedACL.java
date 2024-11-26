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

/**
 * Enum to map AWS SDK V1 Acl values to SDK V2.
 */
public enum AWSCannedACL {
  Private("private"),
  PublicRead("public-read"),
  PublicReadWrite("public-read-write"),
  AuthenticatedRead("authenticated-read"),
  AwsExecRead("aws-exec-read"),
  BucketOwnerRead("bucket-owner-read"),
  BucketOwnerFullControl("bucket-owner-full-control"),
  LogDeliveryWrite("log-delivery-write");

  private final String value;

  AWSCannedACL(String value){
    this.value = value;
  }

  public String toString() {
    return this.value;
  }
}
