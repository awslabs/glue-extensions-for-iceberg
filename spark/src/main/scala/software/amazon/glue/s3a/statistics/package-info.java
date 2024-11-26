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
 * Statistics collection for the S3A connector: interfaces.
 * This is private, though there's a risk that some extension
 * points (delegation tokens?) may need access to the internal
 * API. Hence the split packaging...with a java 9 module, the
 * implementation classes would be declared internal.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package software.amazon.glue.s3a.statistics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
