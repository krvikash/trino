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
import com.google.inject.Singleton;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;

import java.lang.foreign.MemorySegment;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Singleton
public class LazyCollectorService
        extends StorageCollectorService
{
    private final LazyCollectTxService lazyCollectTxService;
    private final ShapingLoggerFactory shapingLoggerFactory;

    @Inject
    LazyCollectorService(StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            CollectTxService collectTxService,
            LazyCollectTxService lazyCollectTxService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory,
            DictionaryCacheService dictionaryCacheService,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        super(storageEngine,
                bufferAllocator,
                metricsManager,
                collectTxService,
                storageEngineConstants,
                blockFillersFactory,
                dictionaryCacheService,
                shapingLoggerFactory);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.lazyCollectTxService = lazyCollectTxService;
    }

    public boolean useLazyCollect(QueryParams queryParams)
    {
        return queryParams.getNumMatchElements() == 0;
    }

    private LazyCollectorLoaderArgs getLazyLoaderArgs(QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            int weIx,
            int lazyCollectStartRowIndex,
            int numRows)
    {
        QueryParams queryParams = queryArgs.queryParams();
        return new LazyCollectorLoaderArgs(queryParams,
                queryArgs.fileCookie(),
                queryParams.getCollectElementsParamsList().get(weIx),
                new ReadJuffersWarmUpElement(bufferAllocator, true),
                aggregatorArgs.blockFillers().get(weIx),
                lazyCollectStartRowIndex,
                numRows,
                queryArgs.numChunksInRange(),
                queryArgs.chunkSize());
    }

    @Override
    void collectChunk(AggregatorPageArgs aggregatorPageArgs,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        // collect is done in LazyCollectorLoader
    }

    @Override
    public Block[] aggregateBlocks(QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = queryArgs.queryParams().getCollectElementsParamsList();
        int rowsToFill = queryState.getNumRecordsInCurPage();
        Block[] blocks = new Block[collectElementsParamsList.size()];

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            LazyCollectorLoaderArgs lazyCollectorLoaderArgs = getLazyLoaderArgs(queryArgs,
                    aggregatorArgs,
                    weIx,
                    queryState.getTotalNumReadRecords(),
                    rowsToFill);
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, new LazyCollectorLoader(
                    lazyCollectTxService,
                    lazyCollectorLoaderArgs,
                    dictionaryStats,
                    queryArgs.dispatcherPageSourceStats(),
                    shapingLoggerFactory,
                    queryArgs.nativeStats()));
        }
        queryArgs.dispatcherPageSourceStats().addlazy_collect_total_blocks(collectElementsParamsList.size());
        queryArgs.dispatcherPageSourceStats().addcached_read_rows(rowsToFill);
        return blocks;
    }
}
