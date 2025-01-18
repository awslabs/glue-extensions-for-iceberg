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

/**
 * A no-op implementation of {@link MetastoreInstrumentation}
 * which allows metastores to always return an instance
 * when requested.
 */
public class MetastoreInstrumentationImpl implements MetastoreInstrumentation {

  @Override
  public void initialized() {

  }

  @Override
  public void storeClosed() {

  }

  @Override
  public void throttled() {

  }

  @Override
  public void retrying() {

  }

  @Override
  public void recordsDeleted(final int count) {

  }

  @Override
  public void recordsRead(final int count) {

  }

  @Override
  public void recordsWritten(final int count) {

  }

  @Override
  public void directoryMarkedAuthoritative() {

  }

  @Override
  public void entryAdded(final long durationNanos) {

  }
}
