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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import software.amazon.glue.s3a.auth.IAMInstanceCredentialsProvider;
import software.amazon.glue.s3a.auth.NoAwsCredentialsException;

/**
 * This credential provider has jittered between existing and non-existing,
 * but it turns up in documentation enough that it has been restored.
 * It extends {@link IAMInstanceCredentialsProvider} to pick up its
 * bindings, which are currently to use the
 * {@code EC2ContainerCredentialsProviderWrapper} class for IAM and container
 * authentication.
 * <p>
 * When it fails to authenticate, it raises a
 * {@link NoAwsCredentialsException} which can be recognized by retry handlers
 * as a non-recoverable failure.
 * <p>
 * It is implicitly public; marked evolving as we can change its semantics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class SharedInstanceCredentialProvider extends IAMInstanceCredentialsProvider {
}
