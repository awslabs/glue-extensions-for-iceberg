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
package org.apache.iceberg;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;

public class ScanCacheKey {

  private final Expression expression;
  private final Set<Types.NestedField> scanFields;
  private final Set<Integer> scanFieldIds;

  public ScanCacheKey(Expression expression, Schema schema) {
    this.expression = expression;
    this.scanFieldIds = schema.identifierFieldIds();
    this.scanFields = schema.asStruct().fields().stream().collect(Collectors.toSet());
  }

  public Expression getExpression() {
    return expression;
  }

  public boolean canReuseScanCache(Expression newExpression, Schema newSchema) {
    return canReuseExistingSchema(newSchema)
        && GlueTableScanUtil.canReuseExistingScanExpression(expression, newExpression);
  }

  private boolean canReuseExistingSchema(Schema newSchema) {
    if (scanFields.size() < newSchema.asStruct().fields().size()) {
      return false;
    }
    return scanFieldIds.containsAll(newSchema.identifierFieldIds())
        && scanFields.containsAll(newSchema.asStruct().fields());
  }
}
