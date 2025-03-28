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

package software.amazon.glue.s3a.audit.impl;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import software.amazon.glue.s3a.S3AFileStatus;
import software.amazon.glue.s3a.audit.AuditManagerS3A;
import software.amazon.glue.s3a.audit.AuditSpanS3A;
import software.amazon.glue.s3a.audit.OperationAuditor;
import software.amazon.glue.s3a.audit.OperationAuditorOptions;
import org.apache.hadoop.service.CompositeService;

/**
 * Simple No-op audit manager for use before a real
 * audit chain is set up, and for testing.
 * It does have the service lifecycle, so do
 * create a unique instance whenever used.
 */
@InterfaceAudience.Private
public class NoopAuditManagerS3A extends CompositeService
    implements AuditManagerS3A, NoopSpan.SpanActivationCallbacks {

  private static final NoopAuditor NOOP_AUDITOR =
      NoopAuditor.createAndStartNoopAuditor(new Configuration(), null);

  /**
   * The inner auditor.
   */
  private final NoopAuditor auditor = NOOP_AUDITOR;

  /**
   * ID which is returned as a span ID in the audit event
   * callbacks.
   */
  private final String id;

  /**
   * Constructor.
   * Will create and start a new instance of the auditor.
   */
  public NoopAuditManagerS3A() {
    super("NoopAuditManagerS3A");
    id = UUID.randomUUID().toString();
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    NoopAuditor audit = new NoopAuditor(this);
    final OperationAuditorOptions options =
        OperationAuditorOptions.builder()
            .withConfiguration(conf)
            .withIoStatisticsStore(iostatisticsStore().build());
    addService(audit);
    audit.init(options);
  }

  @Override
  public String getSpanId() {
    return id;
  }

  @Override
  public String getOperationName() {
    return getName();
  }

  @Override
  public OperationAuditor getAuditor() {
    return auditor;
  }

  /**
   * Unbonded span to use after deactivation.
   */
  private AuditSpanS3A getUnbondedSpan() {
    return auditor.getUnbondedSpan();
  }

  @Override
  public AuditSpanS3A getActiveAuditSpan() {
    return NoopSpan.INSTANCE;
  }

  @Override
  public AuditSpanS3A createSpan(final String operation,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    return createNewSpan(operation, path1, path2);
  }

  @Override
  public List<RequestHandler2> createRequestHandlers() throws IOException {
    return new ArrayList<>();
  }

  @Override
  public TransferStateChangeListener createStateChangeListener() {
    return new TransferStateChangeListener() {
      public void transferStateChanged(final Transfer transfer,
          final Transfer.TransferState state) {
      }
    };
  }

  /**
   * Forward to the auditor.
   * @param path path to check
   * @param status status of the path.
   * @param mode access mode.
   * @throws IOException failure
   */
  @Override
  public boolean checkAccess(final Path path,
      final S3AFileStatus status,
      final FsAction mode)
      throws IOException {
    return auditor.checkAccess(path, status, mode);
  }

  @Override
  public void activate(final AuditSpanS3A span) {
    /* no-op */
  }

  @Override
  public void deactivate(final AuditSpanS3A span) {
    activate(getUnbondedSpan());
  }

  /**
   * A static source of no-op spans, using the same span ID
   * source as managed spans.
   * @param name operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   */
  public static AuditSpanS3A createNewSpan(
      final String name,
      final String path1,
      final String path2) {
    return NoopSpan.INSTANCE;
  }
}
