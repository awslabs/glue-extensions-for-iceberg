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

import javax.annotation.Nonnull;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.MultipartUploaderBuilderImpl;
import software.amazon.glue.s3a.S3AFileSystem;
import software.amazon.glue.s3a.WriteOperations;
import software.amazon.glue.s3a.statistics.S3AMultipartUploaderStatistics;

/**
 * Builder for S3A multipart uploaders.
 */
public class S3AMultipartUploaderBuilder extends
    MultipartUploaderBuilderImpl<S3AMultipartUploader, S3AMultipartUploaderBuilder> {

  private final WriteOperations writeOperations;

  private final StoreContext context;

  private final S3AMultipartUploaderStatistics statistics;

  public S3AMultipartUploaderBuilder(
      @Nonnull final S3AFileSystem fileSystem,
      @Nonnull final WriteOperations writeOperations,
      @Nonnull final StoreContext context,
      @Nonnull final Path p,
      @Nonnull final S3AMultipartUploaderStatistics statistics) {
    super(fileSystem, p);
    this.writeOperations = writeOperations;
    this.context = context;
    this.statistics = statistics;
  }

  @Override
  public S3AMultipartUploaderBuilder getThisBuilder() {
    return this;
  }

  @Override
  public S3AMultipartUploader build()
      throws IllegalArgumentException, IOException {
    return new S3AMultipartUploader(this, writeOperations, context, statistics);
  }


}
