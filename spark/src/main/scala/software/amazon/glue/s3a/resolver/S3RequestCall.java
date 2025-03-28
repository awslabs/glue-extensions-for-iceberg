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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RequestCall implements S3Call {

  public static final Logger LOG = LoggerFactory.getLogger(S3RequestCall.class);

  private String bucket;

  private List<S3Resource> s3Resources;

  private List<S3Request> s3Requests;

  public S3RequestCall() {
    s3Resources = new ArrayList<>();
    s3Requests = new ArrayList<>();
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public void setS3Resources(List<S3Resource> s3Resources) {
    this.s3Resources = s3Resources;
  }

  public void setS3Requests(List<S3Request> s3Requests) {
    this.s3Requests = s3Requests;
  }

  @Override
  public String getBucketName() {
    return bucket;
  }

  @Override
  public Collection<S3Resource> getS3Resources() {
    return s3Resources;
  }

  @Override
  public List<S3Request> getS3Requests() {
    return s3Requests;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{Bucket: ").append(getBucketName());

    stringBuilder.append(",[S3Requests: ");
    for (S3Request s3Request : getS3Requests()) {
      stringBuilder.append(s3Request.name()).append(" ");
    }
    stringBuilder.append("]");

    stringBuilder.append(",[S3Resource: ");
    for (S3Resource s3Resource : getS3Resources()) {
      stringBuilder.append("(Type: ").append(s3Resource.getType());
      stringBuilder.append(",Bucket Name: ").append(s3Resource.getBucketName());
      stringBuilder.append(",Path: ").append(s3Resource.getPath());
      stringBuilder.append(")");
    }
    stringBuilder.append("]");
    stringBuilder.append("}");
    return stringBuilder.toString();
  }
}
