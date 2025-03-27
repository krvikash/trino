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

import io.trino.plugin.warp.dispatcher.WarpMDCContext;
import io.trino.plugin.warp.gen.constants.QueryResultType;
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
import java.util.List;
import java.util.Optional;

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
        try (WarpMDCContext _ = new WarpMDCContext(lazyCollectorLoaderArgs.catalogName().toString(), Optional.of(lazyCollectorLoaderArgs.queryParams().getQueryId()))) {
            checkState(!loaded, "Already loaded");
            loaded = true;

            Block retBlock;
            LazyCollectOpenResult collectOpenResult = null;

            // open
            try {
                collectOpenResult = collectTxService.collectOpen(lazyCollectorLoaderArgs, dispatcherPageSourceStats);
            }
            catch (Exception e) {
                shapingLogger.error(e, "lazy collectOpen failed LazyCollectorArgs %s collectParams %s, collectOpenResults %s",
                        lazyCollectorLoaderArgs, lazyCollectorLoaderArgs.collectParams(), collectOpenResult);
                dispatcherPageSourceStats.inclazy_collect_failed_load();
                throw e;
            }

            // collect
            List<ChunkProperties> chunkPropertiesList = lazyCollectorLoaderArgs.chunkPropertiesList();
            MemorySegment queryResultTypeMem = collectOpenResult.pageArena().allocate(ValueLayout.JAVA_INT.byteSize(), ValueLayout.JAVA_INT.byteSize());

            for (ChunkProperties chunkProperties : chunkPropertiesList) {
                try {
                    lazyCollectorLoaderArgs.recordIndexes().setCurChunkProperties(chunkProperties);
                    collectTxService.openChunk(collectOpenResult.collectState(),
                            chunkProperties.chunkIndex(),
                            dispatcherPageSourceStats);
                    collectTxService.collectChunk(collectOpenResult.collectState(),
                            queryResultTypeMem,
                            dispatcherPageSourceStats);
                }
                catch (Exception e) {
                    shapingLogger.error(e, "lazy collect failed chunk %s LazyCollectorArgs %s collectParams %s, collectOpenResults %s",
                            chunkProperties, lazyCollectorLoaderArgs, lazyCollectorLoaderArgs.collectParams(), collectOpenResult);
                    dispatcherPageSourceStats.inclazy_collect_failed_load();
                    collectTxService.collectAbort(e, collectOpenResult.collectState(), dispatcherPageSourceStats);
                    throw e;
                }
            }

            // fill block
            WarmupElementCollectParams collectParams = lazyCollectorLoaderArgs.collectParams();
            ReadJuffersWarmUpElement readJuffersWarmUpElement = lazyCollectorLoaderArgs.collectJufferWE();
            QueryResultType queryResultType = QueryResultType.values()[queryResultTypeMem.get(ValueLayout.JAVA_INT, 0)];
            retBlock = lazyCollectorLoaderArgs.blockFiller().fillBlockWithRecords(collectParams,
                    readJuffersWarmUpElement,
                    lazyCollectorLoaderArgs.numToCollect(),
                    queryResultType,
                    dictionaryStats,
                    dispatcherPageSourceStats);

            // close
            try {
                collectTxService.collectClose(collectOpenResult, nativeStats, dispatcherPageSourceStats);
                dispatcherPageSourceStats.inclazy_collect_loaded_blocks();
            }
            catch (Exception e) {
                shapingLogger.error(e, "lazy collect failed in close");
            }
            return retBlock;
        }
    }
}
