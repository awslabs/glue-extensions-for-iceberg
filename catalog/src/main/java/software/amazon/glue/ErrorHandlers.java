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
package software.amazon.glue;

import java.util.function.Consumer;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.glue.exceptions.ErrorResponse;
import software.amazon.glue.exceptions.ErrorResponseParser;
import software.amazon.glue.exceptions.GlueExtensionsException;
import software.amazon.glue.exceptions.NoSuchTransactionException;

/**
 * A set of consumers to handle errors for requests for table entities or for namespace entities, to
 * throw the correct exception.
 */
public class ErrorHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlers.class);

  private ErrorHandlers() {}

  public static Consumer<ErrorResponse> catalogErrorHandler() {
    return CatalogErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> namespaceErrorHandler() {
    return NamespaceErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> tableErrorHandler() {
    return TableErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> tableCommitHandler() {
    return CommitErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> defaultErrorHandler() {
    return DefaultErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> transactionErrorHandler() {
    return TransactionErrorHandler.INSTANCE;
  }

  /** Table commit error handler. */
  private static class CommitErrorHandler extends DefaultErrorHandler {
    private static final ErrorHandler INSTANCE = new CommitErrorHandler();

    @Override
    public void accept(ErrorResponse error) {
      switch (error.code()) {
        case 404:
          throw new NoSuchTableException("%s", error.message());
        case 409:
          throw new CommitFailedException("Commit failed: %s", error.message());
        case 500:
        case 502:
        case 504:
          throw new CommitStateUnknownException(
              new ServiceFailureException("Service failed: %s: %s", error.code(), error.message()));
      }

      super.accept(error);
    }
  }

  /** Table level error handler. */
  private static class TableErrorHandler extends DefaultErrorHandler {
    private static final ErrorHandler INSTANCE = new TableErrorHandler();

    @Override
    public void accept(ErrorResponse error) {
      switch (error.code()) {
        case 404:
          if (NoSuchNamespaceException.class.getSimpleName().equals(error.type())) {
            throw new NoSuchNamespaceException("%s", error.message());
          } else {
            throw new NoSuchTableException("%s", error.message());
          }
        case 409:
          throw new AlreadyExistsException("%s", error.message());
      }

      super.accept(error);
    }
  }

  /** Request error handler specifically for CRUD ops on catalogs. */
  private static class CatalogErrorHandler extends DefaultErrorHandler {
    private static final ErrorHandler INSTANCE = new CatalogErrorHandler();

    @Override
    public void accept(ErrorResponse error) {
      switch (error.code()) {
        case 404:
          throw new NoSuchNamespaceException("%s", error.message());
        case 409:
          throw new AlreadyExistsException("%s", error.message());
        case 422:
          throw new GlueExtensionsException("Unable to process: %s", error.message());
      }

      super.accept(error);
    }
  }

  /** Request error handler specifically for CRUD ops on namespaces. */
  private static class NamespaceErrorHandler extends DefaultErrorHandler {
    private static final ErrorHandler INSTANCE = new NamespaceErrorHandler();

    @Override
    public void accept(ErrorResponse error) {
      switch (error.code()) {
        case 404:
          throw new NoSuchNamespaceException("%s", error.message());
        case 409:
          throw new AlreadyExistsException("%s", error.message());
        case 422:
          throw new GlueExtensionsException("Unable to process: %s", error.message());
      }

      super.accept(error);
    }
  }

  /** transaction level error handler. */
  private static class TransactionErrorHandler extends DefaultErrorHandler {
    private static final ErrorHandler INSTANCE = new TransactionErrorHandler();

    @Override
    public void accept(ErrorResponse error) {
      if (error.code() == 404) {
        throw new NoSuchTransactionException("%s", error.message());
      }
      super.accept(error);
    }
  }

  /**
   * Request error handler that handles the common cases that are included with all responses, such
   * as 400, 500, etc.
   */
  private static class DefaultErrorHandler extends ErrorHandler {
    private static final ErrorHandler INSTANCE = new DefaultErrorHandler();

    @Override
    public ErrorResponse parseResponse(int code, String json) {
      try {
        return ErrorResponseParser.fromJson(json);
      } catch (Exception x) {
        LOG.warn("Unable to parse error response", x);
      }
      return ErrorResponse.builder().responseCode(code).withMessage(json).build();
    }

    @Override
    public void accept(ErrorResponse error) {
      switch (error.code()) {
        case 400:
          if (IllegalArgumentException.class.getSimpleName().equals(error.type())) {
            throw new IllegalArgumentException(error.message());
          }
          throw new BadRequestException("Malformed request: %s", error.message());
        case 401:
          throw new NotAuthorizedException("Not authorized: %s", error.message());
        case 403:
          throw new ForbiddenException("Forbidden: %s", error.message());
        case 405:
        case 406:
          break;
        case 500:
          throw new ServiceFailureException("Server error: %s: %s", error.type(), error.message());
        case 501:
          throw new UnsupportedOperationException(error.message());
        case 503:
          throw new ServiceUnavailableException("Service unavailable: %s", error.message());
      }

      throw new GlueExtensionsException("Unable to process: %s", error.message());
    }
  }
}
