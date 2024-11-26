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
 * Extensible delegation token support for the S3A connector.
 *
 * Goal: support multiple back end token issue/renewal services, from
 * "pure client side" session tokens to full "Kerberos auth".
 *
 * It is intended for internal use only; any external implementation
 * of {@link org.apache.hadoop.fs.s3a.auth.delegation.AbstractDelegationTokenBinding}
 * must consider this API unstable and track changes as they happen.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
package software.amazon.glue.s3a.auth.delegation;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
