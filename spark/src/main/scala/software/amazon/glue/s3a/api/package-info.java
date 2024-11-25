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
 * Where the interfaces for classes implemented in {@code o.a.h.fs.s3a.impl}
 * should go. This is to allow for extension points to use these interfaces
 * without having any java module access to the .impl package.
 *
 * This is public for S3A extension points, however there are no
 * guarantees of stability -changes may break things, possibly
 * unintentionally.
 */

@InterfaceAudience.LimitedPrivate("extensions")
@InterfaceStability.Unstable
package software.amazon.glue.s3a.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
