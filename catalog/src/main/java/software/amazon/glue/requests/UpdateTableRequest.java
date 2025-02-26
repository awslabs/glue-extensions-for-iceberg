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
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import software.amazon.glue.GlueRequest;

public class UpdateTableRequest implements GlueRequest {

  private TableIdentifier identifier;
  private List<org.apache.iceberg.UpdateRequirement> requirements;
  private List<MetadataUpdate> updates;

  public UpdateTableRequest() {
    // needed for Jackson deserialization
  }

  public UpdateTableRequest(
      List<org.apache.iceberg.UpdateRequirement> requirements, List<MetadataUpdate> updates) {
    this.requirements = requirements;
    this.updates = updates;
  }

  UpdateTableRequest(
      TableIdentifier identifier,
      List<org.apache.iceberg.UpdateRequirement> requirements,
      List<MetadataUpdate> updates) {
    this(requirements, updates);
    this.identifier = identifier;
  }

  @Override
  public void validate() {}

  public List<org.apache.iceberg.UpdateRequirement> requirements() {
    return requirements != null ? requirements : ImmutableList.of();
  }

  public List<MetadataUpdate> updates() {
    return updates != null ? updates : ImmutableList.of();
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("requirements", requirements)
        .add("updates", updates)
        .toString();
  }

  public static UpdateTableRequest create(
      TableIdentifier identifier,
      List<org.apache.iceberg.UpdateRequirement> requirements,
      List<MetadataUpdate> updates) {
    return new UpdateTableRequest(identifier, requirements, updates);
  }
}
