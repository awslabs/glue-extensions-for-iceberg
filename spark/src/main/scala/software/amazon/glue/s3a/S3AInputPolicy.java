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

import static software.amazon.glue.s3a.Constants.*;

import java.util.Locale;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filesystem input policy.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum S3AInputPolicy {

  Normal(INPUT_FADV_NORMAL),
  Sequential(INPUT_FADV_SEQUENTIAL),
  Random(INPUT_FADV_RANDOM);

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AInputPolicy.class);
  private final String policy;

  S3AInputPolicy(String policy) {
    this.policy = policy;
  }

  @Override
  public String toString() {
    return policy;
  }

  /**
   * Choose an FS access policy.
   * Always returns something,
   * primarily by downgrading to "normal" if there is no other match.
   * @param name strategy name from a configuration option, etc.
   * @return the chosen strategy
   */
  public static S3AInputPolicy getPolicy(String name) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    switch (trimmed) {
    case INPUT_FADV_NORMAL:
      return Normal;
    case INPUT_FADV_RANDOM:
      return Random;
    case INPUT_FADV_SEQUENTIAL:
      return Sequential;
    default:
      LOG.warn("Unrecognized " + INPUT_FADVISE + " value: \"{}\"", trimmed);
      return Normal;
    }
  }

}
