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

import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * Simple enum to express {true, false, don't know}.
 */
public enum Tristate {

  // Do not add additional values here.  Logic will assume there are exactly
  // three possibilities.
  TRUE(of(Boolean.TRUE)),
  FALSE(of(Boolean.FALSE)),
  UNKNOWN(empty());

  /**
   *  Mapping to an optional boolean.
   */
  @SuppressWarnings("NonSerializableFieldInSerializableClass")
  private final Optional<Boolean> mapping;

  Tristate(final Optional<Boolean> t) {
    mapping = t;
  }

  /**
   * Get the boolean mapping, if present.
   * @return the boolean value, if present.
   */
  public Optional<Boolean> getMapping() {
    return mapping;
  }

  /**
   * Does this value map to a boolean.
   * @return true if the state is one of true or false.
   */
  public boolean isBoolean() {
    return mapping.isPresent();
  }

  public static Tristate fromBool(boolean v) {
    return v ? TRUE : FALSE;
  }

  /**
   * Build a tristate from a boolean.
   * @param b source optional
   * @return a tristate derived from the argument.
   */
  public static Tristate from(Optional<Boolean> b) {
    return b.map(Tristate::fromBool).orElse(UNKNOWN);
  }
}
