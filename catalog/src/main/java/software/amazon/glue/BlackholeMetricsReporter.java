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

import java.util.Map;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;

/**
 * Behaves like /dev/null. Unlike the logging metrics reporter, this reporter do not print anything,
 * and prevents accidental leak of information.
 */
public class BlackholeMetricsReporter implements MetricsReporter {

  private static final MetricsReporter INSTANCE = new BlackholeMetricsReporter();

  public static MetricsReporter instance() {
    return INSTANCE;
  }

  @Override
  public void report(MetricsReport report) {}

  @Override
  public void initialize(Map<String, String> properties) {}
}
