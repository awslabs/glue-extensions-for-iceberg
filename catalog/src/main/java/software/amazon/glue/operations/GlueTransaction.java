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
package software.amazon.glue.operations;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;

public class GlueTransaction implements Transaction {
  private GlueTable table;
  private Transaction delegate;

  public GlueTransaction(GlueTable table, Transaction delegate) {
    this.table = table;
    this.delegate = delegate;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public UpdateSchema updateSchema() {
    return delegate.updateSchema();
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return delegate.updateSpec();
  }

  @Override
  public UpdateProperties updateProperties() {
    return delegate.updateProperties();
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return delegate.replaceSortOrder();
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Unsupported: updateLocation");
  }

  @Override
  public AppendFiles newAppend() {
    return table.newAppend();
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException("Unsupported: newRewrite");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException("Unsupported: rewriteManifests");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return table.newOverwrite();
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException("Unsupported: newRowDelta");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException("Unsupported: newReplacePartitions");
  }

  @Override
  public DeleteFiles newDelete() {
    return table.newDelete();
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException("Unsupported: expireSnapshots");
  }

  @Override
  public void commitTransaction() {
    delegate.commitTransaction();
  }
}
