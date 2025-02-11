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

import com.amazonaws.AmazonServiceException;

/**
 * A 500 response came back from a service.
 * This is considered <i>probably</i> retriable, That is, we assume
 * <ol>
 *   <li>whatever error happened in the service itself to have happened
 *    before the infrastructure committed the operation.</li>
 *    <li>Nothing else got through either.</li>
 * </ol>
 */
public class AWSStatus500Exception extends AWSServiceIOException {
  public AWSStatus500Exception(String operation,
      AmazonServiceException cause) {
    super(operation, cause);
  }
}
