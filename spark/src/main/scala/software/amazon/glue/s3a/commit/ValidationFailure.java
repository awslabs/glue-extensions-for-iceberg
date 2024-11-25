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

package software.amazon.glue.s3a.commit;

import java.io.IOException;

/**
 * Exception raised on validation failures; kept as an IOException
 * for consistency with other failures.
 */
public class ValidationFailure extends IOException {

  /**
   * Create an instance with string formatting applied to the message
   * and arguments.
   * @param message error message
   * @param args optional list of arguments
   */
  public ValidationFailure(String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Verify that a condition holds.
   * @param expression expression which must be true
   * @param message message to raise on a failure
   * @param args arguments for the message formatting
   * @throws ValidationFailure on a failure
   */
  public static void verify(boolean expression,
      String message,
      Object... args) throws ValidationFailure {
    if (!expression) {
      throw new ValidationFailure(message, args);
    }
  }
}
