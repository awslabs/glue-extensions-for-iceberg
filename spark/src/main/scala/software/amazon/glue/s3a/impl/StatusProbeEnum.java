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

package software.amazon.glue.s3a.impl;

import java.util.EnumSet;
import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Enum of probes which can be made of S3.
 */
@InterfaceAudience.Private
public enum StatusProbeEnum {

  /** The actual path. */
  Head,
  /** HEAD of the path + /. */
  DirMarker,
  /** LIST under the path. */
  List;

  /** Look for files and directories. */
  public static final Set<StatusProbeEnum> ALL =
      EnumSet.of(Head, List);

  /** We only want the HEAD. */
  public static final Set<StatusProbeEnum> HEAD_ONLY =
      EnumSet.of(Head);

  /** List operation only. */
  public static final Set<StatusProbeEnum> LIST_ONLY =
      EnumSet.of(List);

  /** Look for files and directories. */
  public static final Set<StatusProbeEnum> FILE =
      HEAD_ONLY;

  /** Skip the HEAD and only look for directories. */
  public static final Set<StatusProbeEnum> DIRECTORIES =
      LIST_ONLY;
}
