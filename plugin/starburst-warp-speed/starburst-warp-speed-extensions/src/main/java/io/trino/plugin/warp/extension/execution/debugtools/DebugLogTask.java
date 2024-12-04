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
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.plugin.warp.extension.execution.debugtools.DebugLogTask.TASK_NAME;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(shouldCheckExecutionAllowed = false)
@Path(TASK_NAME)
//@Api(value = "Debug Log", tags = "Debug")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DebugLogTask
        implements TaskResource
{
    public static final String TASK_NAME = "debug-log";

    private static final Logger logger = Logger.get(DebugLogTask.class);

    private static final JsonCodec<DebugLogData> jsonCodec = JsonCodec.jsonCodec(DebugLogData.class);

    private final NodeManager nodeManager;
    private final WarpClient warpClient;

    @Inject
    public DebugLogTask(
            NodeManager nodeManager,
            WarpClient warpClient)
    {
        this.nodeManager = requireNonNull(nodeManager);
        this.warpClient = requireNonNull(warpClient);
    }

    @POST
//@ApiOperation(value = "debug-log", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public void debugLog(DebugLogData debugLogData)
    {
        logger.info(debugLogData.getLogLine());
        if (nodeManager.getCurrentNode().isCoordinator()) {
            nodeManager.getWorkerNodes()
                    .stream()
                    .filter(Node::isCoordinator) // in case of single node cluster
                    .forEach(node -> {
                        HttpUriBuilder uriBuilder = warpClient.getRestEndpoint(UriUtils.getHttpUri(node));
                        uriBuilder.appendPath(DebugLogTask.TASK_NAME);

                        Request request = preparePost()
                                .setUri(uriBuilder.build())
                                .setBodyGenerator(jsonBodyGenerator(jsonCodec, debugLogData))
                                .setHeader("Content-Type", "application/json")
                                .build();
                        warpClient.sendWithRetry(request, createFullJsonResponseHandler(JsonCodec.jsonCodec(Void.class)));
                    });
        }
    }
}
