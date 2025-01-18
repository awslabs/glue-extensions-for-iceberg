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

package software.amazon.glue.s3a.s3guard;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * All the capability constants used for the
 * {@link MetadataStore} implementations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class MetadataStoreCapabilities {

  private MetadataStoreCapabilities(){
  }

  /**
   *  This capability tells if the metadata store supports authoritative
   *  directories. Used in {@link MetadataStore#getDiagnostics()} as a key
   *  for this capability. The value can be boolean true or false.
   *  If the Map.get() returns null for this key, that is interpreted as false.
   */
  public static final String PERSISTS_AUTHORITATIVE_BIT =
      "persist.authoritative.bit";
}
