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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.node.CoordinatorNodeManager;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.spi.Node;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

@Singleton
@Path(NativeStorageStateTask.PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@TaskResourceMarker(worker = false)
public class NativeStorageStateTask
        implements TaskResource
{
    public static final String PATH = "native_storage_state";

    private static final Logger logger = Logger.get(NativeStorageStateTask.class);
    private static final JsonCodec<NativeStorageState> nativeStorageStateCodec = JsonCodec.jsonCodec(NativeStorageState.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final WarpClient warpClient;

    @Inject
    public NativeStorageStateTask(
            CoordinatorNodeManager coordinatorNodeManager,
            WarpClient warpClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.warpClient = requireNonNull(warpClient);
    }

    @GET
    public List<NativeStorageState> get()
    {
        List<NativeStorageState> result = new ArrayList<>();
        try {
            List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
            workerNodes.forEach(workerNode -> {
                HttpUriBuilder uriBuilder = warpClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
                uriBuilder.appendPath(NativeStorageStateResource.PATH);
                Request request = prepareGet()
                        .setUri(uriBuilder.build())
                        .setHeader(CONTENT_TYPE, "application/json")
                        .build();
                NativeStorageState nativeStorageState = warpClient.sendWithRetry(request, createFullJsonResponseHandler(nativeStorageStateCodec));
                result.add(nativeStorageState);
            });
        }
        catch (Exception e) {
            logger.error(e, "failed to get NativeStorageState");
        }
        return result;
    }
}
