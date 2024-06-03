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
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.WorkerNodeManager;
import io.trino.plugin.warp.dictionary.DebugDictionaryKey;
import io.trino.plugin.warp.dictionary.DebugDictionaryMetadata;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dictionary.DictionaryCacheService.DictionaryCacheConfig;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.TrinoException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.Map;

@TaskResourceMarker(coordinator = false)
@Path(WorkerDictionaryCountTask.WORKER_DICTIONARY_GROUP_PATH)
//@Api(value = "Worker Dictionary", tags = "Dictionary")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerDictionaryCountTask
        implements TaskResource
{
    public static final String WORKER_DICTIONARY_GROUP_PATH = "worker-dictionary-group";
    public static final String WORKER_DICTIONARY_RESET_TASK_NAME = "worker-dictionary-memory-reset";
    public static final String WORKER_DICTIONARY_PATH = "worker-dictionary-count";
    public static final String WORKER_DICTIONARY_GET_CONFIGURATION = "worker-dictionary-get-config";
    public static final String WORKER_DICTIONARY_GET_CACHED_KEYS = "worker-dictionary-get-cache-keys";
    public static final String WORKER_DICTIONARY_USAGE_PATH = "worker-dictionary-usage";
    private static final Logger logger = Logger.get(WorkerDictionaryCountTask.class);
    private final DictionaryCacheService dictionaryCacheService;
    private final WorkerNodeManager workerNodeManager;

    @Inject
    public WorkerDictionaryCountTask(DictionaryCacheService dictionaryCacheService,
            WorkerNodeManager workerNodeManager)
    {
        this.dictionaryCacheService = dictionaryCacheService;
        this.workerNodeManager = workerNodeManager;
    }

    @Path(WORKER_DICTIONARY_RESET_TASK_NAME)
    @POST
    public int reset(DictionaryConfigRequest config)
    {
        validateConfig(config);
        return dictionaryCacheService.resetMemoryDictionaries(config.getMaxDictionaryTotalCacheWeight(), config.getConcurrency());
    }

    private void validateConfig(DictionaryConfigRequest conf)
    {
        if (conf.getMaxDictionaryTotalCacheWeight() < 0) {
            throw new TrinoException(WarpErrorCode.VARADA_ILLEGAL_PARAMETER, "maxDictionaryWeight should be greater then -1");
        }
        if (conf.getConcurrency() < 0) {
            throw new TrinoException(WarpErrorCode.VARADA_ILLEGAL_PARAMETER, "Concurrency should be greater then -1");
        }
    }

    @Path(WORKER_DICTIONARY_PATH)
    @POST
    public WorkerDictionaryCountResult count()
    {
        return innerCount();
    }

    private WorkerDictionaryCountResult innerCount()
    {
        Map<DebugDictionaryKey, DebugDictionaryMetadata> workerDictionaryMetadata = dictionaryCacheService.getWorkerDictionaryMetadata();
        return new WorkerDictionaryCountResult(new ArrayList<>(workerDictionaryMetadata.values()), workerNodeManager.getCurrentNodeIdentifier());
    }

    @Path(WORKER_DICTIONARY_USAGE_PATH)
    @GET
    public int dictionariesUsage()
    {
        logger.error("deprecated not to use");
        return 0;
    }

    @Path(WORKER_DICTIONARY_GET_CONFIGURATION)
    @GET
    public DictionaryConfigResult getDictionaryConfig()
    {
        DictionaryCacheConfig dictionaryCacheConfig = dictionaryCacheService.getDictionaryConfig();
        return new DictionaryConfigResult(dictionaryCacheConfig.dictionaryMaxSize(), dictionaryCacheConfig.concurrency(), dictionaryCacheConfig.maxDictionaryTotalCacheWeight());
    }

    @Path(WORKER_DICTIONARY_GET_CACHED_KEYS)
    @GET
    public Map<String, Integer> getDictionaryCachedKeys()
    {
        return dictionaryCacheService.getDictionaryCachedKeys();
    }
}
