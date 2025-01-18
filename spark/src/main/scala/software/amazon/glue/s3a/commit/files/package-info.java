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
 * Persistent data formats for the committers.
 *
 * All of these formats share a base class of
 * {@link software.amazon.glue.s3a.commit.files.PersistentCommitData};
 * the subclasses record
 * <ol>
 *   <li>The content of a single pending commit
 *   (used by the Magic committer).</li>
 *   <li>The list of all the files uploaded by a staging committer.</li>
 *   <li>The summary information saved in the {@code _SUCCESS} file.</li>
 * </ol>
 *
 * There are no guarantees of stability between versions; these are internal
 * structures.
 *
 * The {@link software.amazon.glue.s3a.commit.files.SuccessData} file is
 * the one visible to callers after a job completes; it is an unstable
 * manifest intended for testing only.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package software.amazon.glue.s3a.commit.files;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
