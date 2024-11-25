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

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

/**
 * This enum is to centralize the encryption methods and
 * the value required in the configuration.
 */
public enum S3AEncryptionMethods {

  NONE("", false, false),
  SSE_S3("AES256", true, false),
  SSE_KMS("SSE-KMS", true, false),
  SSE_C("SSE-C", true, true),
  CSE_KMS("CSE-KMS", false, true),
  CSE_CUSTOM("CSE-CUSTOM", false, true),
  DSSE_KMS("DSSE-KMS", true, false);

  /**
   * Error string when {@link #getMethod(String)} fails.
   * Used in tests.
   */
  public static final String UNKNOWN_ALGORITHM
      = "Unknown encryption algorithm ";

  /**
   * What is the encryption method?
   */
  private final String method;

  /**
   * Is this server side?
   */
  private final boolean serverSide;

  /**
   * Does the encryption method require a
   * secret in the encryption.key property?
   */
  private final boolean requiresSecret;

  S3AEncryptionMethods(String method,
      final boolean serverSide,
      final boolean requiresSecret) {
    this.method = method;
    this.serverSide = serverSide;
    this.requiresSecret = requiresSecret;
  }

  public String getMethod() {
    return method;
  }

  /**
   * Flag to indicate this is a server-side encryption option.
   * @return true if this is server side.
   */
  public boolean isServerSide() {
    return serverSide;
  }

  /**
   * Does this encryption algorithm require a secret?
   * @return true if a secret must be retrieved.
   */
  public boolean requiresSecret() {
    return requiresSecret;
  }

  /**
   * Get the encryption mechanism from the value provided.
   * @param name algorithm name
   * @return the method
   * @throws IOException if the algorithm is unknown
   */
  public static S3AEncryptionMethods getMethod(String name) throws IOException {
    if(StringUtils.isBlank(name)) {
      return NONE;
    }
    for (S3AEncryptionMethods v : values()) {
      if (v.getMethod().equalsIgnoreCase(name)) {
        return v;
      }
    }
    throw new IOException(UNKNOWN_ALGORITHM + name);
  }

}
