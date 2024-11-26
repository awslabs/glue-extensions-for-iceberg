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

import software.amazon.glue.s3a.S3AInputStream;

import java.util.List;
import java.util.function.IntFunction;

/**
 * Context related to vectored IO operation.
 * See {@link S3AInputStream#readVectored(List, IntFunction)}.
 */
public class VectoredIOContext {

  /**
   * What is the smallest reasonable seek that we should group
   * ranges together during vectored read operation.
   */
  private int minSeekForVectorReads;

  /**
   * What is the largest size that we should group ranges
   * together during vectored read operation.
   * Setting this value 0 will disable merging of ranges.
   */
  private int maxReadSizeForVectorReads;

  /**
   * Default no arg constructor.
   */
  public VectoredIOContext() {
  }

  public VectoredIOContext setMinSeekForVectoredReads(int minSeek) {
    this.minSeekForVectorReads = minSeek;
    return this;
  }

  public VectoredIOContext setMaxReadSizeForVectoredReads(int maxSize) {
    this.maxReadSizeForVectorReads = maxSize;
    return this;
  }

  public VectoredIOContext build() {
    return this;
  }

  public int getMinSeekForVectorReads() {
    return minSeekForVectorReads;
  }

  public int getMaxReadSizeForVectorReads() {
    return maxReadSizeForVectorReads;
  }

  @Override
  public String toString() {
    return "VectoredIOContext{" +
            "minSeekForVectorReads=" + minSeekForVectorReads +
            ", maxReadSizeForVectorReads=" + maxReadSizeForVectorReads +
            '}';
  }
}
