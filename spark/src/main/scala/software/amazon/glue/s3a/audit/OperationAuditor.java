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

package software.amazon.glue.s3a.audit;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import software.amazon.glue.s3a.S3AFileStatus;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.service.Service;

/**
 * Interfaces for audit services to implement.
 */
public interface OperationAuditor extends Service,
    IOStatisticsSource, AuditSpanSource<AuditSpanS3A> {

  /**
   * Initialize.
   * The base class will call {@link Service#init(Configuration)}.
   * @param options options to initialize with.
   */
  void init(OperationAuditorOptions options);

  /**
   * Get the unbonded span to use after deactivating an active
   * span.
   * @return a span.
   */
  AuditSpanS3A getUnbondedSpan();

  /**
   * Check for permission to access a path.
   * The path is fully qualified and the status is the
   * status of the path.
   * This is called from the {@code FileSystem.access()} command
   * and is a soft permission check used by Hive.
   * @param path path to check
   * @param status status of the path.
   * @param mode access mode.
   * @return true if access is allowed.
   * @throws IOException failure
   */
  default boolean checkAccess(Path path, S3AFileStatus status, FsAction mode)
      throws IOException {
    return true;
  }

  /**
   * Get the Auditor ID.
   * @return ID
   */
  String getAuditorId();
}
