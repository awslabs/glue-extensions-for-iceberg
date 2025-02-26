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

import static software.amazon.glue.s3a.Constants.S3GUARD_DDB_REGION_KEY;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import software.amazon.glue.s3a.Constants;
import software.amazon.glue.s3a.S3AUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to create a DynamoDB client.
 *
 * Implementation should be configured for setting and getting configuration.
 */
@InterfaceAudience.Private
public interface DynamoDBClientFactory extends Configurable {
  Logger LOG = LoggerFactory.getLogger(DynamoDBClientFactory.class);

  /**
   * Create a DynamoDB client object from configuration.
   *
   * The DynamoDB client to create does not have to relate to any S3 buckets.
   * All information needed to create a DynamoDB client is from the hadoop
   * configuration. Specially, if the region is not configured, it will use the
   * provided region parameter. If region is neither configured nor provided,
   * it will indicate an error.
   *
   * @param defaultRegion the default region of the AmazonDynamoDB client
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param credentials credentials to use for authentication.
   * @return a new DynamoDB client
   * @throws IOException if any IO error happens
   */
  AmazonDynamoDB createDynamoDBClient(final String defaultRegion,
      final String bucket,
      final AWSCredentialsProvider credentials) throws IOException;

  /**
   * The default implementation for creating an AmazonDynamoDB.
   */
  class DefaultDynamoDBClientFactory extends Configured
      implements DynamoDBClientFactory {
    @Override
    public AmazonDynamoDB createDynamoDBClient(String defaultRegion,
        final String bucket,
        final AWSCredentialsProvider credentials)
        throws IOException {
      Preconditions.checkNotNull(getConf(),
          "Should have been configured before usage");

      final Configuration conf = getConf();
      final ClientConfiguration awsConf = S3AUtils
          .createAwsConf(conf, bucket, Constants.AWS_SERVICE_IDENTIFIER_DDB);

      final String region = getRegion(conf, defaultRegion);
      LOG.debug("Creating DynamoDB client in region {}", region);

      return AmazonDynamoDBClientBuilder.standard()
          .withCredentials(credentials)
          .withClientConfiguration(awsConf)
          .withRegion(region)
          .build();
    }

    /**
     * Helper method to get and validate the AWS region for DynamoDBClient.
     *
     * @param conf configuration
     * @param defaultRegion the default region
     * @return configured region or else the provided default region
     * @throws IOException if the region is not valid
     */
    static String getRegion(Configuration conf, String defaultRegion)
        throws IOException {
      String region = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
      if (StringUtils.isEmpty(region)) {
        region = defaultRegion;
      }
      try {
        Regions.fromName(region);
      } catch (IllegalArgumentException | NullPointerException e) {
        throw new IOException("Invalid region specified: " + region + "; " +
            "Region can be configured with " + S3GUARD_DDB_REGION_KEY + ": " +
            validRegionsString());
      }
      return region;
    }

    private static String validRegionsString() {
      final String delimiter = ", ";
      Regions[] regions = Regions.values();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < regions.length; i++) {
        if (i > 0) {
          sb.append(delimiter);
        }
        sb.append(regions[i].getName());
      }
      return sb.toString();

    }
  }

}
