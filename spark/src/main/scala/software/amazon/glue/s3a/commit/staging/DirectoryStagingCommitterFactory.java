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

package software.amazon.glue.s3a.commit.staging;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.S3AFileSystem;
import software.amazon.glue.s3a.commit.AbstractS3ACommitterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

/**
 * Factory for the Directory committer.
 */
public class DirectoryStagingCommitterFactory
    extends AbstractS3ACommitterFactory {

  /**
   * Name of this class: {@value}.
   */
  public static final String CLASSNAME
      = "software.amazon.glue.s3a.commit.staging"
      + ".DirectoryStagingCommitterFactory";

  public PathOutputCommitter createTaskCommitter(S3AFileSystem fileSystem,
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    return new DirectoryStagingCommitter(outputPath, context);
  }

}
