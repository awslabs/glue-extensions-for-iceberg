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
package software.amazon.glue;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

/** Values that can be returned from LoadTable API config */
public class GlueLoadTableConfig implements Serializable {

  public static final String SERVER_SIDE_SCAN_PLANNING_ENABLED =
      "aws.server-side-capabilities.scan-planning";
  public static final boolean SERVER_SIDE_SCAN_PLANNING_ENABLED_DEFAULT = false;
  public static final String SERVER_SIDE_DATA_COMMIT_ENABLED =
      "aws.server-side-capabilities.data-commit";
  public static final boolean SERVER_SIDE_DATA_COMMIT_ENABLED_DEFAULT = false;
  /** Property name for S3 staging location. */
  public static final String STAGING_LOCATION = "aws.glue.staging.location";
  /** Property name for AWS access key ID of the staging location. */
  public static final String STAGING_ACCESS_KEY_ID = "aws.glue.staging.access-key-id";
  /** Property name for AWS secret access key of the staging location. */
  public static final String STAGING_SECRET_ACCESS_KEY = "aws.glue.staging.secret-access-key";
  /** Property name for AWS session token of the staging location. */
  public static final String STAGING_SESSION_TOKEN = "aws.glue.staging.session-token";
  /** Property name for AWS credential expiration timestamp of the staging location. */
  public static final String STAGING_EXPIRATION_MS = "aws.glue.staging.expiration-ms";
  /** Property name for AWS data transfer role for staging location. */
  public static final String STAGING_DATA_TRANSFER_ROLE_ARN =
      "aws.glue.staging.data-transfer-role-arn";

  private final boolean serverSideScanPlanningEnabled;
  private final boolean serverSideDataCommitEnabled;
  private final String stagingLocation;
  private String stagingAccessKeyId;
  private String stagingSecretAccessKey;
  private String stagingSessionToken;
  private Instant stagingExpirationInstant;
  private String stagingDataTransferRole;

  public GlueLoadTableConfig(Map<String, String> config) {
    this.serverSideScanPlanningEnabled =
        PropertyUtil.propertyAsBoolean(
            config, SERVER_SIDE_SCAN_PLANNING_ENABLED, SERVER_SIDE_SCAN_PLANNING_ENABLED_DEFAULT);
    this.serverSideDataCommitEnabled =
        PropertyUtil.propertyAsBoolean(
            config, SERVER_SIDE_DATA_COMMIT_ENABLED, SERVER_SIDE_DATA_COMMIT_ENABLED_DEFAULT);
    this.stagingLocation = config.get(STAGING_LOCATION);
    if ((serverSideScanPlanningEnabled || serverSideDataCommitEnabled) && stagingLocation != null) {
      this.stagingDataTransferRole = config.get(STAGING_DATA_TRANSFER_ROLE_ARN);
      Preconditions.checkNotNull(
          stagingDataTransferRole, "AWS Glue staging data transfer role cannot be null");
      this.stagingAccessKeyId = config.get(STAGING_ACCESS_KEY_ID);
      Preconditions.checkNotNull(
          stagingAccessKeyId, "AWS Glue staging access key ID cannot be null");
      this.stagingSecretAccessKey = config.get(STAGING_SECRET_ACCESS_KEY);
      Preconditions.checkNotNull(
          stagingSecretAccessKey, "AWS Glue staging secrete access key cannot be null");
      this.stagingSessionToken = config.get(STAGING_SESSION_TOKEN);
      Preconditions.checkNotNull(
          stagingSessionToken, "AWS Glue staging session token cannot be null");
      this.stagingExpirationInstant =
          Instant.ofEpochMilli(PropertyUtil.propertyAsNullableLong(config, STAGING_EXPIRATION_MS));
      Preconditions.checkNotNull(
          stagingExpirationInstant, "AWS Glue staging expiration timestamp cannot be null");
    }
  }

  public String stagingLocation() {
    return stagingLocation;
  }

  public String stagingDataTransferRole() {
    return stagingDataTransferRole;
  }

  public boolean serverSideScanPlanningEnabled() {
    return serverSideScanPlanningEnabled;
  }

  public boolean serverSideDataCommitEnabled() {
    return serverSideDataCommitEnabled;
  }

  public String stagingAccessKeyId() {
    return stagingAccessKeyId;
  }

  public String stagingSecretAccessKey() {
    return stagingSecretAccessKey;
  }

  public String stagingSessionToken() {
    return stagingSessionToken;
  }

  public Instant stagingExpirationMs() {
    return stagingExpirationInstant;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GlueLoadTableConfig{");
    sb.append("serverSideScanPlanningEnabled=").append(serverSideScanPlanningEnabled);
    sb.append(", serverSideDataCommitEnabled=").append(serverSideDataCommitEnabled);
    sb.append(", stagingLocation='").append(stagingLocation).append('\'');
    sb.append(", stagingAccessKeyId='").append(stagingAccessKeyId).append('\'');
    sb.append(", stagingSecretAccessKey='").append(stagingSecretAccessKey).append('\'');
    sb.append(", stagingSessionToken='").append(stagingSessionToken).append('\'');
    sb.append(", stagingExpirationInstant=").append(stagingExpirationInstant);
    sb.append(", stagingDataTransferRole='").append(stagingDataTransferRole).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
