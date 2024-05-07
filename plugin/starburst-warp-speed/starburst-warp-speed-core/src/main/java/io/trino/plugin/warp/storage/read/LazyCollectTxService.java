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
package io.trino.plugin.warp.storage.read;

import com.google.inject.Inject;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.storage.engine.StorageEngine;

import static com.google.common.base.Preconditions.checkState;

public class LazyCollectTxService
        extends BaseCollectTxService
{
    @Inject
    public LazyCollectTxService(StorageEngine storageEngine, GlobalConfig globalConfig)
    {
        super(storageEngine, globalConfig);
    }

    LazyCollectOpenResult collectOpen(int rowsLimit, StorageCollectorArgs storageCollectorArgs)
    {
        int numCollectElements = storageCollectorArgs.collectParamsList().size();
        checkState(numCollectElements == 1, "lazy collector should collect only one WE at a time");
        long[] metadataBuffIds = new long[2];
        int[] outResultType = new int[numCollectElements];
        int collectTxId = collectOpen(storageCollectorArgs, 0, metadataBuffIds, outResultType);

        logger.debug("collectOpen collectTxId %d rowsLimit %d", collectTxId, rowsLimit);
        return new LazyCollectOpenResult(collectTxId, outResultType);
    }

    // Lazy collect doesn't use store/restore mechanism, so store/restore params are not initialized
    void collectClose(int collectTxId)
    {
        storageEngine.collectClose(collectTxId, null, 0, null);
    }
}
