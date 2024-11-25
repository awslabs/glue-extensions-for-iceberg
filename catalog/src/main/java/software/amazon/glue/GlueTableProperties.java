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

public class GlueTableProperties {

  public static final String AWS_WRITE_FORMAT = "aws.write.format";
  public static final String AWS_WRITE_FORMAT_RMS = "rms";
  public static final String AWS_WRITE_DISTRIBUTION_STYLE = "aws.write.distribution-style";
  public static final String AWS_WRITE_SORT_KEYS = "aws.write.sort-keys";
  public static final String AWS_WRITE_PRIMARY_KEYS = "aws.write.primary-keys";
  public static final String WRITE_LOCATION_PROVIDER_REST_TABLE_PATH =
      "write.location-provider.rest-table-path";
  public static final String WRITE_LOCATION_PROVIDER_STAGING_LOCATION =
      "write.location-provider.staging-location";
}
