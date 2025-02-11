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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.fs.store.audit.AuditSpan;

/**
 * Base class of operations in the store.
 * An operation is something which executes against the context to
 * perform a single function.
 */
public abstract class AbstractStoreOperation {

  /**
   * Store context.
   */
  private final StoreContext storeContext;

  /**
   * Audit Span.
   */
  private AuditSpan auditSpan;

  /**
   * Constructor.
   * Picks up the active audit span from the store context and
   * stores it for later.
   * @param storeContext store context.
   */
  protected AbstractStoreOperation(final StoreContext storeContext) {
    this(storeContext, storeContext.getActiveAuditSpan());
  }

  /**
   * Constructor.
   * @param storeContext store context.
   * @param auditSpan active span
   */
  protected AbstractStoreOperation(final StoreContext storeContext,
      final AuditSpan auditSpan) {
    this.storeContext = checkNotNull(storeContext);
    this.auditSpan = checkNotNull(auditSpan);
  }

  /**
   * Get the store context.
   * @return the context.
   */
  public final StoreContext getStoreContext() {
    return storeContext;
  }

  /**
   * Get the audit span this object was created with.
   * @return the current span
   */
  public AuditSpan getAuditSpan() {
    return auditSpan;
  }

  /**
   * Activate the audit span.
   */
  public void activateAuditSpan() {
    auditSpan.activate();
  }
}
