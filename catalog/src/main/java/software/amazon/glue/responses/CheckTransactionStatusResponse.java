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

public class CheckTransactionStatusResponse implements GlueResponse {
  private Status status;
  private String error;

  public CheckTransactionStatusResponse() {
    // Needed for Jackson Deserialization.
  }

  public CheckTransactionStatusResponse(Status status, String error) {
    this.status = status;
    this.error = error;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(status != null, "Invalid status: null");
  }

  public Status status() {
    return status;
  }

  public String error() {
    return error;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("status", status).add("error", error).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Status status;
    private String error;

    private Builder() {}

    public Builder withStatus(Status status) {
      this.status = status;
      return this;
    }

    public Builder withError(String error) {
      this.error = error;
      return this;
    }

    public CheckTransactionStatusResponse build() {
      return new CheckTransactionStatusResponse(status, error);
    }
  }

  public enum Status {
    STARTED,
    FINISHED,
    FAILED,
    CANCELED
  }
}
