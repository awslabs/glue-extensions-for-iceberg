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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GlueFileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import software.amazon.glue.ErrorHandlers;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueExtensionsPaths;
import software.amazon.glue.GlueExtensionsProperties;
import software.amazon.glue.requests.PlanTableRequest;

public class PlanTableIterable implements CloseableIterable<FileScanTask> {
  private final PlanTableRequest request;
  private final GlueExtensionsClient client;
  private final String planTablePath;
  private final GlueExtensionsProperties properties;

  public PlanTableIterable(
      PlanTableRequest request,
      GlueExtensionsClient client,
      String planTablePath,
      GlueExtensionsProperties properties) {
    Preconditions.checkArgument(request != null, "request must not be null");
    this.request = request;
    this.client = client;
    this.planTablePath = planTablePath;
    this.properties = properties;
  }

  @Override
  public CloseableIterator<FileScanTask> iterator() {
    return new PlanTableIterator(request, client, planTablePath, properties);
  }

  @Override
  public void close() throws IOException {}

  public static class PlanTableIterator implements CloseableIterator<FileScanTask> {
    private final GlueExtensionsClient client;
    private final String planTablePath;
    private final PlanTableRequest request;
    private final GlueExtensionsProperties properties;
    private List<FileScanTask> fileScanTasks;
    private int fileScanTaskIdx = 0;
    private String pageToken = null;

    public PlanTableIterator(
        PlanTableRequest request,
        GlueExtensionsClient client,
        String planTablePath,
        GlueExtensionsProperties properties) {
      this.request = request;
      this.client = client;
      this.planTablePath = planTablePath;
      this.properties = properties;
    }

    @Override
    public boolean hasNext() {
      if (fileScanTasks == null) {
        fetchNextBatch();
        return hasNext();
      }

      if (fileScanTaskIdx == fileScanTasks.size()) {
        if (pageToken != null) {
          fetchNextBatch();
          return hasNext();
        }
        return false;
      }

      return true;
    }

    @Override
    public FileScanTask next() {
      return fileScanTasks.get(fileScanTaskIdx++);
    }

    private void fetchNextBatch() {
      PlanTableResponse planTableResponse =
          client.post(
              planTablePath,
              pageToken != null
                  ? ImmutableMap.of(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, pageToken)
                  : ImmutableMap.of(),
              request,
              PlanTableResponse.class,
              ErrorHandlers.defaultErrorHandler());

      pageToken = planTableResponse.nextPageToken();
      if (pageToken != null && planTableResponse.fileScanTasks().isEmpty()) {
        try {
          Thread.sleep(properties.scanPlanningStatusCheckIntervalMs());
          return;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      fileScanTasks =
          planTableResponse.fileScanTasks().stream()
              .map(GlueFileScanTask::from)
              .collect(Collectors.toList());
      fileScanTaskIdx = 0;
    }

    @Override
    public void close() throws IOException {}
  }
}
