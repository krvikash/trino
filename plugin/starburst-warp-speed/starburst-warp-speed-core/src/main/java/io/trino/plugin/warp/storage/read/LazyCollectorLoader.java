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

import io.airlift.log.Logger;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlockLoader;

import static com.google.common.base.Preconditions.checkState;

public class LazyCollectorLoader
        implements LazyBlockLoader
{
    private static final Logger logger = Logger.get(StorageCollectorService.class);
    private final LazyCollectTxService collectTxService;
    private final LazyCollectorArgs lazyCollectorArgs;
    private final DispatcherPageSourceStats dispatcherPageSourceStats;
    private final DictionaryStats dictionaryStats;
    private boolean loaded;

    public LazyCollectorLoader(
            LazyCollectTxService collectTxService,
            LazyCollectorArgs lazyCollectorArgs,
            DictionaryStats varadaStatsDictionary,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        this.collectTxService = collectTxService;
        this.dispatcherPageSourceStats = dispatcherPageSourceStats;
        this.dictionaryStats = varadaStatsDictionary;
        this.lazyCollectorArgs = lazyCollectorArgs;
    }

    @Override
    public Block load()
    {
        checkState(!loaded, "Already loaded");
        loaded = true;

        Block retBlock;
        int chunkToCollect = lazyCollectorArgs.chunkIx();
        int rowsToCollect = lazyCollectorArgs.numToCollect();
        try {
            LazyCollectOpenResult collectOpenResult = collectTxService.collectOpen(rowsToCollect, lazyCollectorArgs);
            collectTxService.prepareNextChunk(chunkToCollect,
                    rowsToCollect,
                    false,
                    collectOpenResult.collectTxId(),
                    rowsToCollect,
                    0,
                    collectOpenResult.outResultType());

            collectTxService.collect(collectOpenResult.collectTxId(), collectOpenResult.outResultType(), 1, chunkToCollect, rowsToCollect);
            WarmupElementCollectParams collectParams = lazyCollectorArgs.collectParams();
            ReadJuffersWarmUpElement readJuffersWarmUpElement = lazyCollectorArgs.collectJufferWE();
            QueryResultType queryResultType = QueryResultType.values()[collectOpenResult.outResultType()[0]];
            retBlock = lazyCollectorArgs.blockFiller().fillBlockWithRecords(collectParams, readJuffersWarmUpElement, rowsToCollect, queryResultType, dictionaryStats);
            collectTxService.collectClose(collectOpenResult.collectTxId());
            dispatcherPageSourceStats.addlazy_collect_loaded_blocks(1);
        }
        catch (Exception e) {
            logger.error(e, "lazy collect failed chunkIndex %d numToCollect %d", chunkToCollect, rowsToCollect);
            throw e;
        }
        return retBlock;
    }
}
