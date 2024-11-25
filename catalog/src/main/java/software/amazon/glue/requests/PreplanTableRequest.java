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
package software.amazon.glue.requests;

import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import software.amazon.glue.GlueRequest;

@Value.Immutable
public interface PreplanTableRequest extends GlueRequest {

  @Nullable
  Long snapshotId();

  @Nullable
  List<String> select();

  @Nullable
  String filter();

  @Value.Default
  default boolean metricsRequested() {
    return false;
  }

  @Override
  default void validate() {
    // nothing to validate as it's not possible to create an invalid instance
  }
}
