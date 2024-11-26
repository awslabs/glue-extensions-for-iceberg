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

package software.amazon.glue.s3a;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import static software.amazon.glue.s3a.Constants.*;

/**
 * Simple object which stores current failure injection settings.
 */
public class FailureInjectionPolicy {
  /**
   * Keys containing this substring will be subject to delayed visibility.
   */
  public static final String DEFAULT_DELAY_KEY_SUBSTRING = "DELAY_LISTING_ME";

  private static final Logger LOG =
      LoggerFactory.getLogger(FailureInjectionPolicy.class);

  /**
   * Probability of throttling a request.
   */
  private float throttleProbability;

  /**
   * limit for failures before operations succeed; if 0 then "no limit".
   */
  private int failureLimit = 0;

  public FailureInjectionPolicy(Configuration conf) {

    this.setThrottleProbability(conf.getFloat(FAIL_INJECT_THROTTLE_PROBABILITY,
        0.0f));
  }

  public float getThrottleProbability() {
    return throttleProbability;
  }

  public int getFailureLimit() {
    return failureLimit;
  }

  public void setFailureLimit(int failureLimit) {
    this.failureLimit = failureLimit;
  }

  /**
   * Set the probability of throttling a request.
   * @param throttleProbability the probability of a request being throttled.
   */
  public void setThrottleProbability(float throttleProbability) {
    this.throttleProbability = validProbability(throttleProbability);
  }

  public static boolean trueWithProbability(float p) {
    return Math.random() < p;
  }

  @Override
  public String toString() {
    return String.format("FailureInjectionPolicy:" +
            " throttle probability %s" + "; failure limit %d",
        throttleProbability, failureLimit);
  }

  /**
   * Validate a probability option.
   * @param p probability
   * @return the probability, if valid
   * @throws IllegalArgumentException if the probability is out of range.
   */
  private static float validProbability(float p) {
    Preconditions.checkArgument(p >= 0.0f && p <= 1.0f,
        "Probability out of range 0 to 1 %s", p);
    return p;
  }

}
