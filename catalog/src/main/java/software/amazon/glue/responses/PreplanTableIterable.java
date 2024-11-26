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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import software.amazon.glue.ErrorHandlers;
import software.amazon.glue.GlueExtensionsClient;
import software.amazon.glue.GlueExtensionsPaths;
import software.amazon.glue.GlueExtensionsProperties;
import software.amazon.glue.requests.PreplanTableRequest;

public class PreplanTableIterable implements CloseableIterable<String> {

  private final PreplanTableRequest request;
  private final GlueExtensionsClient client;
  private final String preplanTablePath;
  private final GlueExtensionsProperties properties;

  public PreplanTableIterable(
      PreplanTableRequest request,
      GlueExtensionsClient client,
      String preplanTablePath,
      GlueExtensionsProperties properties) {
    this.request = request;
    this.client = client;
    this.preplanTablePath = preplanTablePath;
    this.properties = properties;
  }

  @Override
  public CloseableIterator<String> iterator() {
    return new PreplanTableIterator(request, client, preplanTablePath, properties);
  }

  @Override
  public void close() throws IOException {}

  public static class PreplanTableIterator implements CloseableIterator<String> {

    private final PreplanTableRequest request;
    private final GlueExtensionsClient client;
    private final String preplanTablePath;
    private final GlueExtensionsProperties properties;
    private List<String> shards;
    private int shardIdx = 0;
    private String pageToken = null;

    public PreplanTableIterator(
        PreplanTableRequest request,
        GlueExtensionsClient client,
        String preplanTablePath,
        GlueExtensionsProperties properties) {
      this.request = request;
      this.client = client;
      this.preplanTablePath = preplanTablePath;
      this.properties = properties;
    }

    @Override
    public boolean hasNext() {
      if (shards == null) {
        fetchNextBatch();
        return hasNext();
      }

      if (shardIdx == shards.size()) {
        if (pageToken != null) {
          fetchNextBatch();
          return hasNext();
        }
        return false;
      }

      return true;
    }

    @Override
    public String next() {
      return shards.get(shardIdx++);
    }

    private void fetchNextBatch() {
      PreplanTableResponse planTableResponse =
          client.post(
              preplanTablePath,
              pageToken != null
                  ? ImmutableMap.of(GlueExtensionsPaths.QUERY_PARAM_PAGE_TOKEN, pageToken)
                  : ImmutableMap.of(),
              request,
              PreplanTableResponse.class,
              ErrorHandlers.defaultErrorHandler());

      pageToken = planTableResponse.nextPageToken();
      if (pageToken != null && planTableResponse.shards().isEmpty()) {
        try {
          Thread.sleep(properties.scanPlanningStatusCheckIntervalMs());
          return;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      shards = planTableResponse.shards();
      shardIdx = 0;
    }

    @Override
    public void close() throws IOException {}
  }
}
