/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.extension.execution.warmup;

import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.warp.annotation.Audit;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.node.CoordinatorNodeManager;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import io.trino.spi.Node;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false, cacheMgr = true, connector = false)
@Path(CacheMgrWarmupTask.CACHE_MANAGER_WARMUP_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CacheMgrWarmupTask
        implements TaskResource
{
    public static final String CACHE_MANAGER_WARMUP_PATH = "cache-manager-warmup";
    public static final String TASK_NAME_FETCH = "run-fetcher";

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final WarpClient warpClient;

    @Inject
    public CacheMgrWarmupTask(
            CoordinatorNodeManager coordinatorNodeManager,
            WarpClient warpClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.warpClient = requireNonNull(warpClient);
    }

    @Path(TASK_NAME_FETCH)
    @GET
    @Audit
    public Map<String, List<CacheManagerRule>> fetch()
    {
        List<Node> workers = coordinatorNodeManager.getWorkerNodes();
        return workers.stream()
                .parallel()
                .map(node -> {
                    HttpUriBuilder uriBuilder = warpClient.getRestEndpoint(UriUtils.getHttpUri(node));
                    uriBuilder.appendPath(WorkerCacheMgrWarmupTask.WORKER_WARMUP_PATH).appendPath(WorkerCacheMgrWarmupTask.TASK_NAME_FETCH);

                    Request request = prepareGet()
                            .setUri(uriBuilder.build())
                            .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                            .build();
                    List<CacheManagerRule> warmupRules = warpClient.sendWithRetry(request, createFullJsonResponseHandler(JsonCodec.listJsonCodec(CacheManagerRule.class)));
                    return Pair.of(node.getNodeIdentifier(), warmupRules);
                }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }
}
