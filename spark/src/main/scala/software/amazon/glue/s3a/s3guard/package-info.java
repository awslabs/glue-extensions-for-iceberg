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
 * This package contains classes related to S3Guard: a feature of S3A to mask
 * the eventual consistency behavior of S3 and optimize access patterns by
 * coordinating with a strongly consistent external store for file system
 * metadata.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
package software.amazon.glue.s3a.s3guard;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
