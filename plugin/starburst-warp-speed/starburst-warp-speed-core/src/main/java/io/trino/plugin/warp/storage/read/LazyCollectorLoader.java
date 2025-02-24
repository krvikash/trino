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

import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlockLoader;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static com.google.common.base.Preconditions.checkState;

public class LazyCollectorLoader
        implements LazyBlockLoader
{
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
            ShapingLoggerFactory shapingLoggerFactory,
            NativeStats nativeStats)
    {
        this.collectTxService = collectTxService;
        this.dispatcherPageSourceStats = dispatcherPageSourceStats;
        this.dictionaryStats = varadaStatsDictionary;
        this.lazyCollectorLoaderArgs = lazyCollectorLoaderArgs;
        this.shapingLogger = shapingLoggerFactory.getInstance(LazyCollectorLoader.class);
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
        ChunkProperties chunkProperties = new ChunkProperties(chunkIndexToCollect, numRowsToCollect, RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL, startRowIndexInChunk);
        LazyCollectOpenResult collectOpenResult = null;
        try {
            // open
            collectOpenResult = collectTxService.collectOpen(lazyCollectorLoaderArgs, dispatcherPageSourceStats);

            // prepare and collect
            lazyCollectorLoaderArgs.recordIndexes().setCurChunkProperties(chunkProperties);
            MemorySegment queryResultTypeMem = collectOpenResult.pageArena().allocate(ValueLayout.JAVA_INT.byteSize(), ValueLayout.JAVA_INT.byteSize());
            collectTxService.openChunk(collectOpenResult.collectState(),
                    chunkIndexToCollect,
                    dispatcherPageSourceStats);
            collectTxService.collectChunk(collectOpenResult.collectState(),
                    queryResultTypeMem,
                    dispatcherPageSourceStats);

            // fill block
            WarmupElementCollectParams collectParams = lazyCollectorLoaderArgs.collectParams();
            ReadJuffersWarmUpElement readJuffersWarmUpElement = lazyCollectorLoaderArgs.collectJufferWE();
            QueryResultType queryResultType = QueryResultType.values()[queryResultTypeMem.get(ValueLayout.JAVA_INT, 0)];
            retBlock = lazyCollectorLoaderArgs.blockFiller().fillBlockWithRecords(collectParams,
                    readJuffersWarmUpElement,
                    numRowsToCollect,
                    queryResultType,
                    dictionaryStats,
                    dispatcherPageSourceStats);
        }
        catch (Exception e) {
            shapingLogger.error(e, "lazy collect failed chunk %s LazyCollectorArgs %s collectParams %s, collectOpenResults %s",
                    chunkProperties, lazyCollectorLoaderArgs, lazyCollectorLoaderArgs.collectParams(), collectOpenResult);
            dispatcherPageSourceStats.inclazy_collect_failed_load();
            if (collectOpenResult != null) {
                collectTxService.collectAbort(e, collectOpenResult.collectState(), dispatcherPageSourceStats);
            }
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
