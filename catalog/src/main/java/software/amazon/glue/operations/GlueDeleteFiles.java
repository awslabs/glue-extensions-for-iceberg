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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.expressions.Expression;

public class GlueDeleteFiles extends GlueUpdateFilesOperation<DeleteFiles> implements DeleteFiles {

  private Expression deleteExpression = null;

  public GlueDeleteFiles(GlueTable table) {
    super(table);
  }

  @Override
  protected DeleteFiles self() {
    return this;
  }

  @Override
  public DeleteFiles deleteFile(CharSequence filePath) {
    throw new UnsupportedOperationException("Delete from filepath is not supported");
  }

  @Override
  public DeleteFiles deleteFile(DataFile file) {
    return remove(file);
  }

  @Override
  public DeleteFiles deleteFromRowFilter(Expression expr) {
    this.deleteExpression = expr;
    return this;
  }

  @Override
  public DeleteFiles caseSensitive(boolean caseSensitive) {
    throw new UnsupportedOperationException(
        "Setting case sensitivity for delete operation is not supported");
  }

  @Override
  public void commit() {
    if (deleteExpression != null) {
      commitWithDeleteFilterRequest(deleteExpression);
    } else {
      super.commit();
    }
  }
}
