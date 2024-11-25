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
package software.amazon.glue.responses;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.glue.GlueResponse;

public class TransactionStartedResponse implements GlueResponse {

  private String transaction;

  public TransactionStartedResponse() {
    // Needed for Jackson Deserialization.
  }

  public TransactionStartedResponse(String transaction) {
    this.transaction = transaction;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(transaction != null, "Invalid transaction: null");
  }

  public String transaction() {
    return transaction;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("transaction", transaction).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String transaction;

    private Builder() {}

    public TransactionStartedResponse.Builder transaction(String transaction) {
      this.transaction = transaction;
      return this;
    }

    public TransactionStartedResponse build() {
      return new TransactionStartedResponse(transaction);
    }
  }
}
