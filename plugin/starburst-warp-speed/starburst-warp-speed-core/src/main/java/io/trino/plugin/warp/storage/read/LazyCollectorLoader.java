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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlockLoader;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static com.google.common.base.Preconditions.checkState;

public class LazyCollectorLoader
        implements LazyBlockLoader
{
    private static final Logger logger = Logger.get(LazyCollectorLoader.class);
    private final LazyCollectTxService collectTxService;
    private final LazyCollectorLoaderArgs lazyCollectorLoaderArgs;
    private final DispatcherPageSourceStats dispatcherPageSourceStats;
    private final DictionaryStats dictionaryStats;
    private final ShapingLogger shapingLogger;
    private final NativeStats nativeStats;
    private boolean loaded;

    public LazyCollectorLoader(
            LazyCollectTxService collectTxService,
            LazyCollectorLoaderArgs lazyCollectorLoaderArgs,
            DictionaryStats varadaStatsDictionary,
            DispatcherPageSourceStats dispatcherPageSourceStats,
            GlobalConfig globalConfig,
            NativeStats nativeStats)
    {
        this.collectTxService = collectTxService;
        this.dispatcherPageSourceStats = dispatcherPageSourceStats;
        this.dictionaryStats = varadaStatsDictionary;
        this.lazyCollectorLoaderArgs = lazyCollectorLoaderArgs;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        this.nativeStats = nativeStats;
    }

    @Override
    public Block load()
    {
        checkState(!loaded, "Already loaded");
        loaded = true;

        Block retBlock;
        int chunkIndexToCollect = lazyCollectorLoaderArgs.lazyCollectStartRowIndex() / lazyCollectorLoaderArgs.chunkSize();
        int startRowIndexInChunk = lazyCollectorLoaderArgs.lazyCollectStartRowIndex() % lazyCollectorLoaderArgs.chunkSize();
        int numRowsToCollect = lazyCollectorLoaderArgs.numToCollect();
        int readerId = BaseCollectTxService.INVALID_READER_ID;
        LazyCollectOpenResult collectOpenResult = null;
        try {
            // open
            collectOpenResult = collectTxService.collectOpen(numRowsToCollect, lazyCollectorLoaderArgs, dispatcherPageSourceStats);
            readerId = collectOpenResult.readerId();

            // prepare and collect
            MemorySegment queryResultTypeMem = lazyCollectorLoaderArgs.queryResultTypes();
            collectTxService.prepareChunkFullScan(readerId, chunkIndexToCollect, numRowsToCollect, startRowIndexInChunk, dispatcherPageSourceStats);
            collectTxService.collectChunk(readerId, 1, chunkIndexToCollect, numRowsToCollect, queryResultTypeMem, dispatcherPageSourceStats);

            // fill block
            WarmupElementCollectParams collectParams = lazyCollectorLoaderArgs.collectParams();
            ReadJuffersWarmUpElement readJuffersWarmUpElement = lazyCollectorLoaderArgs.collectJufferWE();
            QueryResultType queryResultType = QueryResultType.values()[queryResultTypeMem.get(ValueLayout.JAVA_INT, 0)];
            retBlock = lazyCollectorLoaderArgs.blockFiller().fillBlockWithRecords(collectParams, readJuffersWarmUpElement, numRowsToCollect, queryResultType, dictionaryStats, dispatcherPageSourceStats);
        }
        catch (Exception e) {
            shapingLogger.error(e, "lazy collect failed LazyCollectorArgs %s collectParams %s, collectOpenResults %s",
                    lazyCollectorLoaderArgs, lazyCollectorLoaderArgs.collectParams(), collectOpenResult);
            dispatcherPageSourceStats.inclazy_collect_failed_load();
            collectTxService.collectAbort(e, readerId, dispatcherPageSourceStats);
            throw e;
        }

        try {
            // close
            collectTxService.collectClose(collectOpenResult, nativeStats, dispatcherPageSourceStats);
            dispatcherPageSourceStats.inclazy_collect_loaded_blocks();
        }
        catch (Exception e) {
            shapingLogger.error(e, "lazy collect failed in close");
        }
        return retBlock;
    }
}
