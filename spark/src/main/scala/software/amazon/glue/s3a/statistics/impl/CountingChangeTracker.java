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

package software.amazon.glue.s3a.statistics.impl;

import java.util.concurrent.atomic.AtomicLong;

import software.amazon.glue.s3a.statistics.ChangeTrackerStatistics;

/**
 * A change tracker which increments an atomic long.
 */
public class CountingChangeTracker implements
    ChangeTrackerStatistics {

  /**
   * The counter which is updated on every mismatch.
   */
  private final AtomicLong counter;

  public CountingChangeTracker(final AtomicLong counter) {
    this.counter = counter;
  }

  public CountingChangeTracker() {
    this(new AtomicLong());
  }

  @Override
  public void versionMismatchError() {
    counter.incrementAndGet();
  }

  @Override
  public long getVersionMismatches() {
    return counter.get();
  }
}
