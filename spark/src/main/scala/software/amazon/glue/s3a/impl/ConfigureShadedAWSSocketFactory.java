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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory;
import java.io.IOException;
import javax.net.ssl.HostnameVerifier;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

/**
 * This interacts with the Shaded httpclient library used in the full
 * AWS SDK. If the S3A client is used with the unshaded SDK, this
 * class will not link.
 */
public class ConfigureShadedAWSSocketFactory implements
    NetworkBinding.ConfigureAWSSocketFactory {

  @Override
  public void configureSocketFactory(final ClientConfiguration awsConf,
      final DelegatingSSLSocketFactory.SSLChannelMode channelMode)
      throws IOException {
    DelegatingSSLSocketFactory.initializeDefaultFactory(channelMode);
    awsConf.getApacheHttpClientConfig().setSslSocketFactory(
        new SSLConnectionSocketFactory(
            DelegatingSSLSocketFactory.getDefaultFactory(),
            (HostnameVerifier) null));
  }
}
