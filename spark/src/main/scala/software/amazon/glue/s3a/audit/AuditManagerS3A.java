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

import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import software.amazon.glue.s3a.S3AFileStatus;
import org.apache.hadoop.fs.store.audit.ActiveThreadSpanSource;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.service.Service;

/**
 * Interface for Audit Managers auditing operations through the
 * AWS libraries.
 * The Audit Manager is the binding between S3AFS and the instantiated
 * plugin point -it adds:
 * <ol>
 *   <li>per-thread tracking of audit spans </li>
 *   <li>The wiring up to the AWS SDK</li>
 *   <li>State change tracking for copy operations (does not address issue)</li>
 * </ol>
 */
@InterfaceAudience.Private
public interface AuditManagerS3A extends Service,
    AuditSpanSource<AuditSpanS3A>,
    AWSAuditEventCallbacks,
    ActiveThreadSpanSource<AuditSpanS3A> {

  /**
   * Get the auditor; valid once initialized.
   * @return the auditor.
   */
  OperationAuditor getAuditor();

  /**
   * Create the request handler(s) for this audit service.
   * The list returned is mutable; new handlers may be added.
   * @return list of handlers for the SDK.
   * @throws IOException failure.
   */
  List<RequestHandler2> createRequestHandlers() throws IOException;

  /**
   * Return a transfer state change callback which
   * fixes the active span context to be that in which
   * the state change listener was created.
   * This can be used to audit the creation of the multipart
   * upload initiation request which the transfer manager
   * makes when a file to be copied is split up.
   * This must be invoked/used within the active span.
   * @return a state change listener.
   */
  TransferStateChangeListener createStateChangeListener();

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
  boolean checkAccess(Path path, S3AFileStatus status, FsAction mode)
      throws IOException;
}
