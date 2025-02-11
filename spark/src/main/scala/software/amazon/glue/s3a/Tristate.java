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

/**
 * Simple enum to express {true, false, don't know}.
 */
public enum Tristate {
  // Do not add additional values here.  Logic will assume there are exactly
  // three possibilities.
  TRUE, FALSE, UNKNOWN;

  public static Tristate fromBool(boolean v) {
    return v ? TRUE : FALSE;
  }
}
