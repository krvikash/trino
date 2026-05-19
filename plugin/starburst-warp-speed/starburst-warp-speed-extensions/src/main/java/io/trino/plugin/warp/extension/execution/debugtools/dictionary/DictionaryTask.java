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
package io.trino.plugin.warp.extension.execution.debugtools.dictionary;

import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.dictionary.DebugDictionaryKey;
import io.trino.plugin.warp.dictionary.DebugDictionaryMetadata;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.node.CoordinatorNodeManager;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.spi.Node;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.plugin.warp.extension.execution.debugtools.dictionary.WorkerDictionaryCountTask.WORKER_DICTIONARY_GROUP_PATH;
import static io.trino.plugin.warp.extension.execution.debugtools.dictionary.WorkerDictionaryCountTask.WORKER_DICTIONARY_RESET_TASK_NAME;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(DictionaryTask.DICTIONARY_PATH)
//@Api(value = "Dictionary", tags = "Dictionary")
public class DictionaryTask
        implements TaskResource
{
    public static final String DICTIONARY_PATH = "dictionary";
    public static final String DICTIONARY_COUNT_TASK_NAME = "dictionary-count";
    public static final String DICTIONARY_USAGE_TASK_NAME = "dictionary-usage";
    public static final String DICTIONARY_RESET_MEMORY_TASK_NAME = "dictionary-memory-reset";
    public static final String DICTIONARY_COUNT_AGGREGATED_TASK_NAME = "dictionary-count-aggregated";
    public static final String DICTIONARY_GET_CONFIGURATION = "dictionary-get-config";
    public static final String DICTIONARY_SET_CONFIGURATION_AND_RESET_CACHE = "set-config-reset-cache";
    public static final String DICTIONARY_GET_CACHE_KEYS = "dictionary-get-cache-keys";
    private static final JsonCodec<WorkerDictionaryCountResult> workerDictionaryCountResultJsonCoded = JsonCodec.jsonCodec(WorkerDictionaryCountResult.class);
    private static final JsonCodec<DictionaryConfigResult> workerDictionaryConfigResultJsonCoded = JsonCodec.jsonCodec(DictionaryConfigResult.class);
    private static final JsonCodec<DictionaryConfigRequest> workerDictionaryConfigRequestJsonCoded = JsonCodec.jsonCodec(DictionaryConfigRequest.class);
    private static final JsonCodec<Map<String, Object>> mapJsonCoded = JsonCodec.mapJsonCodec(String.class, Object.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final DictionaryConfig dictionaryConfig;
    private final WarpClient warpClient;

    @Inject
    public DictionaryTask(
            CoordinatorNodeManager coordinatorNodeManager,
            DictionaryConfig dictionaryConfig,
            WarpClient warpClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.dictionaryConfig = requireNonNull(dictionaryConfig);
        this.warpClient = requireNonNull(warpClient);
    }

    @POST
    @Path(DICTIONARY_RESET_MEMORY_TASK_NAME)
    // @ApiOperation(value = "reset", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public int resetMemoryDictionaries()
    {
        return resetMemoryDictionaries(new DictionaryConfigRequest(dictionaryConfig.getMaxDictionaryTotalCacheWeight(), dictionaryConfig.getDictionaryCacheConcurrencyLevel()));
    }

    @POST
    @Path(DICTIONARY_COUNT_TASK_NAME)
    // @ApiOperation(value = "count", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public DictionaryCountResult count()
    {
        return innerCount(false);
    }

    @GET
    @Path(DICTIONARY_GET_CONFIGURATION)
    // @ApiOperation(value = "get dictionary config", nickname = "dictionaryGetConfig", extensions = {
//            @Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, DictionaryConfigResult> getDictionaryConfig()
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        return workerNodes.stream()
                .collect(Collectors.toMap(Node::getNodeIdentifier, workerNode -> (DictionaryConfigResult) getWorkerResult(workerNode, WorkerDictionaryCountTask.WORKER_DICTIONARY_GET_CONFIGURATION, workerDictionaryConfigResultJsonCoded)));
    }

    @POST
    @Path(DICTIONARY_SET_CONFIGURATION_AND_RESET_CACHE)
    // @ApiOperation(value = "set dictionary config and reset cache", nickname = "dictionarySetConfig", extensions = {
//            @Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public int setDictionaryConfig(DictionaryConfigRequest dictionaryConfig)
    {
        long confTotalWeight = dictionaryConfig.getMaxDictionaryTotalCacheWeight() > -1 ?
                dictionaryConfig.getMaxDictionaryTotalCacheWeight() :
                this.dictionaryConfig.getMaxDictionaryTotalCacheWeight();
        int confConcurrency = dictionaryConfig.getConcurrency() > -1 ?
                dictionaryConfig.getConcurrency() :
                this.dictionaryConfig.getDictionaryCacheConcurrencyLevel();
        return resetMemoryDictionaries(new DictionaryConfigRequest(confTotalWeight, confConcurrency));
    }

    @GET
    @Path(DICTIONARY_GET_CACHE_KEYS)
    // @ApiOperation(value = "get dictionary cached values", nickname = "dictionaryGetCachedKeys", extensions = {
//            @Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, Map<String, Integer>> getDictionaryCachedKeys()
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        return workerNodes.stream()
                .collect(Collectors.toMap(Node::getNodeIdentifier, workerNode -> (Map<String, Integer>) getWorkerResult(workerNode, WorkerDictionaryCountTask.WORKER_DICTIONARY_GET_CACHED_KEYS, mapJsonCoded)));
    }

    @POST
    @Path(DICTIONARY_COUNT_AGGREGATED_TASK_NAME)
    // @ApiOperation(value = "count aggregate", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public DictionaryCountResult countAggregated()
    {
        return innerCount(true);
    }

    private int resetMemoryDictionaries(DictionaryConfigRequest dictionaryConfig)
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        AtomicInteger totalRemoved = new AtomicInteger();
        workerNodes.forEach(workerNode -> {
            Integer res = (Integer) postWorkerResult(
                    workerNode,
                    WORKER_DICTIONARY_RESET_TASK_NAME,
                    workerDictionaryConfigRequestJsonCoded,
                    dictionaryConfig,
                    JsonCodec.jsonCodec(Integer.class));
            totalRemoved.addAndGet(res);
        });
        return totalRemoved.get();
    }

    private DictionaryCountResult innerCount(boolean aggregated)
    {
        List<WorkerDictionaryCountResult> workerDictionaryCountResultList = new ArrayList<>();
        Map<DebugDictionaryKey, DebugDictionaryMetadata> dictionaryNameToDictionaryMetadata = new HashMap<>();
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        workerNodes.forEach(workerNode -> {
            WorkerDictionaryCountResult nodeResult = (WorkerDictionaryCountResult) postWorkerResult(
                    workerNode,
                    WorkerDictionaryCountTask.WORKER_DICTIONARY_PATH,
                    null,
                    null,
                    workerDictionaryCountResultJsonCoded);
            if (aggregated) {
                nodeResult.getDictionaryMetadataList().forEach(dictionaryMetadata ->
                        dictionaryNameToDictionaryMetadata.merge(
                                dictionaryMetadata.dictionaryKey(),
                                dictionaryMetadata,
                                (v1, v2) -> new DebugDictionaryMetadata(v1.dictionaryKey(), v1.dictionarySize() + v2.dictionarySize(), v1.failedWriteCount() + v2.failedWriteCount())));
            }
            else {
                workerDictionaryCountResultList.add(nodeResult);
            }
        });
        DictionaryCountResult result;
        if (aggregated) {
            WorkerDictionaryCountResult aggregated1 = new WorkerDictionaryCountResult(new ArrayList<>(dictionaryNameToDictionaryMetadata.values()), null);
            result = new DictionaryCountResult(List.of(aggregated1));
        }
        else {
            result = new DictionaryCountResult(workerDictionaryCountResultList);
        }
        return result;
    }

    @GET
    @Path(DICTIONARY_USAGE_TASK_NAME)
    // @ApiOperation(value = "count", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public long sumUsage()
    {
        AtomicInteger totalUsage = new AtomicInteger();
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        workerNodes.forEach(workerNode -> {
            HttpUriBuilder uriBuilder = warpClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
            uriBuilder.appendPath(WORKER_DICTIONARY_GROUP_PATH);
            uriBuilder.appendPath(WorkerDictionaryCountTask.WORKER_DICTIONARY_USAGE_PATH);

            Request request = prepareGet()
                    .setUri(uriBuilder.build())
                    .setHeader(CONTENT_TYPE, "application/json")
                    .build();
            int workerUsedPages = warpClient.sendWithRetry(request, createFullJsonResponseHandler(JsonCodec.jsonCodec(Integer.class)));
            totalUsage.addAndGet(workerUsedPages);
        });
        return totalUsage.get();
    }

    private Object postWorkerResult(Node workerNode, String workerTaskPath, JsonCodec jsonCodecRequest, Object input, JsonCodec jsonCodecResponse)
    {
        HttpUriBuilder uriBuilder = warpClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
        uriBuilder.appendPath(WORKER_DICTIONARY_GROUP_PATH);
        uriBuilder.appendPath(workerTaskPath);

        Request.Builder builder = preparePost()
                .setUri(uriBuilder.build())
                .setHeader(CONTENT_TYPE, "application/json");
        if (input != null && jsonCodecRequest != null) {
            builder.setBodyGenerator(jsonBodyGenerator(jsonCodecRequest, input));
        }
        return warpClient.sendWithRetry(builder.build(), createFullJsonResponseHandler(jsonCodecResponse));
    }

    private Object getWorkerResult(Node workerNode, String workerTaskPath, JsonCodec jsonCodecResponse)
    {
        HttpUriBuilder uriBuilder = warpClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
        uriBuilder.appendPath(WORKER_DICTIONARY_GROUP_PATH);
        uriBuilder.appendPath(workerTaskPath);

        Request.Builder builder = prepareGet()
                .setUri(uriBuilder.build())
                .setHeader(CONTENT_TYPE, "application/json");
        return warpClient.sendWithRetry(builder.build(), createFullJsonResponseHandler(jsonCodecResponse));
    }
}
