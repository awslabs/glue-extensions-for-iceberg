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
package software.amazon.glue.operations;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public enum ManifestType {
  ICEBERG_JSON("iceberg-json"),
  REDSHIFT("redshift");

  private final String manifestType;

  ManifestType(String manifestType) {
    this.manifestType = manifestType;
  }

  public static ManifestType fromName(String manifestType) {
    Preconditions.checkArgument(manifestType != null, "ManifestType is null");
    if (ICEBERG_JSON.getManifestType().equalsIgnoreCase(manifestType)) {
      return ICEBERG_JSON;
    } else if (REDSHIFT.getManifestType().equalsIgnoreCase(manifestType)) {
      return REDSHIFT;
    } else {
      throw new IllegalArgumentException("Unknown manifestType: " + manifestType);
    }
  }

  public String getManifestType() {
    return manifestType;
  }
}
