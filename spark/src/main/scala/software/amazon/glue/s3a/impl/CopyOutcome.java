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

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.model.CopyResult;

/**
 * Extracts the outcome of a TransferManager-executed copy operation.
 */
public final class CopyOutcome {

  /**
   * Result of a successful copy.
   */
  private final CopyResult copyResult;

  /** the copy was interrupted. */
  private final InterruptedException interruptedException;

  /**
   * The copy raised an AWS Exception of some form.
   */
  private final SdkBaseException awsException;

  public CopyOutcome(CopyResult copyResult,
      InterruptedException interruptedException,
      SdkBaseException awsException) {
    this.copyResult = copyResult;
    this.interruptedException = interruptedException;
    this.awsException = awsException;
  }

  public CopyResult getCopyResult() {
    return copyResult;
  }

  public InterruptedException getInterruptedException() {
    return interruptedException;
  }

  public SdkBaseException getAwsException() {
    return awsException;
  }

  /**
   * Calls {@code Copy.waitForCopyResult()} to await the result, converts
   * it to a copy outcome.
   * Exceptions caught and
   * @param copy the copy operation.
   * @return the outcome.
   */
  public static CopyOutcome waitForCopy(Copy copy) {
    try {
      CopyResult result = copy.waitForCopyResult();
      return new CopyOutcome(result, null, null);
    } catch (SdkBaseException e) {
      return new CopyOutcome(null, null, e);
    } catch (InterruptedException e) {
      return new CopyOutcome(null, e, null);
    }
  }
}
