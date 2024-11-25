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
package software.amazon.glue.responses;

import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import software.amazon.glue.GlueResponse;

public class LoadTableResponse implements GlueResponse {

  private String metadataLocation;
  private TableMetadata metadata;
  private Map<String, String> config;

  public LoadTableResponse() {
    // Required for Jackson deserialization
  }

  private LoadTableResponse(
      String metadataLocation, TableMetadata metadata, Map<String, String> config) {
    this.metadataLocation = metadataLocation;
    this.metadata = metadata;
    this.config = config;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(metadata, "Invalid metadata: null");
  }

  public String metadataLocation() {
    return metadataLocation;
  }

  public TableMetadata tableMetadata() {
    return TableMetadata.buildFrom(metadata).withMetadataLocation(metadataLocation).build();
  }

  public Map<String, String> config() {
    return config != null ? config : ImmutableMap.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("metadataLocation", metadataLocation)
        .add("metadata", metadata)
        .add("config", config)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String metadataLocation;
    private TableMetadata metadata;
    private final Map<String, String> config = Maps.newHashMap();

    private Builder() {}

    public Builder withTableMetadata(TableMetadata tableMetadata) {
      this.metadataLocation = tableMetadata.metadataFileLocation();
      this.metadata = tableMetadata;
      return this;
    }

    public Builder addConfig(String property, String value) {
      config.put(property, value);
      return this;
    }

    public Builder addAllConfig(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public LoadTableResponse build() {
      Preconditions.checkNotNull(metadata, "Invalid metadata: null");
      return new LoadTableResponse(metadataLocation, metadata, config);
    }
  }
}
