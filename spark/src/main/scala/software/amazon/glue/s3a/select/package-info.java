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

/**
 * Was the location for support for S3 Select.
 * Now removed apart from some constants.f
 * There was a class {@code BlockingEnumeration} which
 * mapped SdkPublisher to an Enumeration.
 * This may be of use elsewhere; it can be retrieved from
 * hadoop commit 8bf72346a59c.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package software.amazon.glue.s3a.select;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;