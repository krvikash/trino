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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;

import java.nio.IntBuffer;
import java.util.List;

@Singleton
public class LazyCollectorService
        extends StorageCollectorService
{
    private final GlobalConfig globalConfig;
    private final LazyCollectTxService lazyCollectTxService;
    private final NativeConfig nativeConfig;

    @Inject
    LazyCollectorService(StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            CollectTxService collectTxService,
            LazyCollectTxService lazyCollectTxService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory,
            NativeConfig nativeConfig,
            GlobalConfig globalConfig)
    {
        super(storageEngine, bufferAllocator, metricsManager, chunksQueueService, rangeFillerService, collectTxService, storageEngineConstants, blockFillersFactory);
        this.globalConfig = globalConfig;
        this.lazyCollectTxService = lazyCollectTxService;
        this.nativeConfig = nativeConfig;
    }

    public boolean useLazyCollect(QueryParams queryParams)
    {
        return queryParams.isLazyCollectEnabled() && (queryParams.getNumMatchElements() == 0);
    }

    private LazyCollectorLoaderArgs getLazyLoaderArgs(StorageCollectorArgs storageCollectorArgs, int weIx, int lazyCollectStartRowIndex, int numRows)
    {
        QueryParams queryParams = storageCollectorArgs.collectTxArgs().queryParams();
        WarmupElementCollectParams collectParams = queryParams.getCollectElementsParamsList().get(weIx);

        int[] weCollectParams = queryParams.dumpSingleCollectParams(collectParams);
        long[][] collectBuffIds = new long[1][];
        collectBuffIds[0] = bufferAllocator.getQueryIdsArray(false);
        byte[] collectStoreBuff = new byte[(int) storageEngine.queryGetCollectStateSize(0)];
        byte[] collect2MatchParams = new byte[storageEngine.queryGetCollect2MatchSize()];
        long[] fileCookieParams = storageCollectorArgs.collectTxArgs().fileCookie();

        CollectTxArgs collectTxArgs = new CollectTxArgs(
                weCollectParams,
                collectBuffIds,
                collectStoreBuff,
                collect2MatchParams,
                queryParams,
                fileCookieParams);

        ReadJuffersWarmUpElement juffersWE = new ReadJuffersWarmUpElement(bufferAllocator, true, false);
        return new LazyCollectorLoaderArgs(
                collectTxArgs,
                collectParams,
                juffersWE,
                storageCollectorArgs.blockFillers().get(weIx),
                lazyCollectStartRowIndex,
                numRows,
                storageCollectorArgs.numChunksInRange(),
                storageCollectorArgs.chunkSize());
    }

    @Override
    void collect(CollectOpenResult collectOpenResult, int numCollectElements, int chunkIndex, int numToCollect, int[] outQueryResultType)
    {
        // collect is done in LazyCollectorLoader
    }

    @Override
    public int getMinForTypeAll(int baseRow, CollectOpenResult collectOpenResult, StorageCollectorArgs storageCollectorArgs, int currentNumCollectedRows)
    {
        // assuming lazy collect is only in full scan and that we have only 1 round per getNextPage so can get numCollectedFromCurrentChunk from numCollectedInPreviousRounds
        int numCollectedFromCurrentChunk = collectOpenResult.numCollectedInPreviousRounds() % storageCollectorArgs.chunkSize();
        return baseRow + numCollectedFromCurrentChunk;
    }

    @Override
    void fillBlocks(Block[] blocks,
            StorageCollectorArgs storageCollectorArgs,
            int rowsToFill,
            int numRowsCollectedInPrevRounds,
            int[] queryResultTypes, // was not filled since collect was not called yet
            DispatcherPageSourceStats stats)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = storageCollectorArgs.collectTxArgs().queryParams().getCollectElementsParamsList();

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            LazyCollectorLoaderArgs lazyCollectorLoaderArgs = getLazyLoaderArgs(storageCollectorArgs, weIx, numRowsCollectedInPrevRounds, rowsToFill);
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, new LazyCollectorLoader(
                    lazyCollectTxService,
                    lazyCollectorLoaderArgs,
                    dictionaryStats,
                    stats,
                    globalConfig));
        }
        stats.addlazy_collect_total_blocks(collectElementsParamsList.size());
    }

    @Override
    int getFreeBytes(WarmupElementRecordBufferState warmupElementRecordBufferState, IntBuffer recordBufferStateBuff)
    {
        // for lazy collect we can use the whole juffer size as we are collecting only one we each cycle
        return nativeConfig.getMaxRecJufferSize();
    }
}
