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

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;

/**
 * API version-independent container for S3 List requests.
 */
public final class S3ListRequest {

  /**
   * Format for the toString() method: {@value}.
   */
  private static final String DESCRIPTION
      = "List %s:/%s delimiter=%s keys=%d requester pays=%s";

  private final ListObjectsRequest v1Request;
  private final ListObjectsV2Request v2Request;

  private S3ListRequest(ListObjectsRequest v1, ListObjectsV2Request v2) {
    v1Request = v1;
    v2Request = v2;
  }

  /**
   * Restricted constructors to ensure v1 or v2, not both.
   * @param request v1 request
   * @return new list request container
   */
  public static S3ListRequest v1(ListObjectsRequest request) {
    return new S3ListRequest(request, null);
  }

  /**
   * Restricted constructors to ensure v1 or v2, not both.
   * @param request v2 request
   * @return new list request container
   */
  public static S3ListRequest v2(ListObjectsV2Request request) {
    return new S3ListRequest(null, request);
  }

  /**
   * Is this a v1 API request or v2?
   * @return true if v1, false if v2
   */
  public boolean isV1() {
    return v1Request != null;
  }

  public ListObjectsRequest getV1() {
    return v1Request;
  }

  public ListObjectsV2Request getV2() {
    return v2Request;
  }

  @Override
  public String toString() {
    if (isV1()) {
      return String.format(DESCRIPTION,
          v1Request.getBucketName(), v1Request.getPrefix(),
          v1Request.getDelimiter(), v1Request.getMaxKeys(),
          v1Request.isRequesterPays());
    } else {
      return String.format(DESCRIPTION,
          v2Request.getBucketName(), v2Request.getPrefix(),
          v2Request.getDelimiter(), v2Request.getMaxKeys(),
          v2Request.isRequesterPays());
    }
  }
}
