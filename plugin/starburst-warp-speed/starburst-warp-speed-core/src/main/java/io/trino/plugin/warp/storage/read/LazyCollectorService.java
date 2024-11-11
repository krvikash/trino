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
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
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
            DictionaryCacheService dictionaryCacheService,
            NativeConfig nativeConfig,
            GlobalConfig globalConfig)
    {
        super(storageEngine,
                bufferAllocator,
                metricsManager,
                chunksQueueService,
                rangeFillerService,
                collectTxService,
                storageEngineConstants,
                blockFillersFactory,
                dictionaryCacheService,
                globalConfig);
        this.globalConfig = globalConfig;
        this.lazyCollectTxService = lazyCollectTxService;
        this.nativeConfig = nativeConfig;
    }

    public boolean useLazyCollect(QueryParams queryParams)
    {
        return queryParams.getNumMatchElements() == 0;
    }

    private LazyCollectorLoaderArgs getLazyLoaderArgs(QueryArgs queryArgs,
            StorageCollectorArgs storageCollectorArgs,
            int weIx,
            int lazyCollectStartRowIndex,
            int numRows)
    {
        QueryParams queryParams = queryArgs.queryParams();
        WarmupElementCollectParams collectParams = queryParams.getCollectElementsParamsList().get(weIx);

        int[] weCollectParams = queryParams.dumpSingleCollectParams(collectParams);
        long[][] collectBuffers = new long[1][];
        collectBuffers[0] = bufferAllocator.getCollectBuffersArray();
        byte[] collectStoreBuff = new byte[(int) storageEngine.queryGetCollectStateSize(0)];
        long[] matchCollectMetadataAddresss = new long[1];

        TxArgs txArgs = new TxArgs(
                weCollectParams,
                collectBuffers,
                queryArgs.txArgs().collectStateBuff(),
                collectStoreBuff,
                matchCollectMetadataAddresss,
                queryArgs.txArgs().fileCookie());

        ReadJuffersWarmUpElement juffersWE = new ReadJuffersWarmUpElement(bufferAllocator, true);
        StructLayout warmupElementAttLayout = WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT;
        return new LazyCollectorLoaderArgs(
                queryParams,
                txArgs,
                collectParams,
                juffersWE,
                storageCollectorArgs.blockFillers().get(weIx),
                storageCollectorArgs.warmUpElementAtts().asSlice(warmupElementAttLayout.byteSize() * weIx, warmupElementAttLayout),
                storageCollectorArgs.recordBufferStates(),
                storageCollectorArgs.recordIndexes(),
                storageCollectorArgs.queryResultTypes(),
                lazyCollectStartRowIndex,
                numRows,
                queryArgs.numChunksInRange(),
                queryArgs.chunkSize());
    }

    @Override
    void collectChunk(CollectOpenResult collectOpenResult,
            int numCollectElements,
            int chunkIndex,
            int numToCollect,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        // collect is done in LazyCollectorLoader
    }

    @Override
    public int getMinForTypeAll(int baseRow, CollectOpenResult collectOpenResult, QueryArgs queryArgs, int currentNumCollectedRows)
    {
        // assuming lazy collect is only in full scan and that we have only 1 round per getNextPage so can get numCollectedFromCurrentChunk from numCollectedInPreviousRounds
        int numCollectedFromCurrentChunk = collectOpenResult.numCollectedInPreviousRounds() % queryArgs.chunkSize();
        return baseRow + numCollectedFromCurrentChunk;
    }

    @Override
    void fillBlocks(Block[] blocks,
            QueryArgs queryArgs,
            StorageCollectorArgs storageCollectorArgs,
            WarpQueryState queryState)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = queryArgs.queryParams().getCollectElementsParamsList();
        int rowsToFill = queryState.getNumRecordsInCurPage();

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            LazyCollectorLoaderArgs lazyCollectorLoaderArgs = getLazyLoaderArgs(queryArgs, storageCollectorArgs, weIx, queryState.getTotalNumReadRecords(), rowsToFill);
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, new LazyCollectorLoader(
                    lazyCollectTxService,
                    lazyCollectorLoaderArgs,
                    dictionaryStats,
                    queryArgs.dispatcherPageSourceStats(),
                    globalConfig,
                    queryArgs.nativeStats()));
        }
        queryArgs.dispatcherPageSourceStats().addlazy_collect_total_blocks(collectElementsParamsList.size());
        queryArgs.dispatcherPageSourceStats().addcached_read_rows(rowsToFill);
    }

    @Override
    int getFreeBytes(WarmupElementRecordBufferState warmupElementRecordBufferState)
    {
        // for lazy collect we can use the whole juffer size as we are collecting only one we each cycle
        return nativeConfig.getMaxRecJufferSize();
    }
}
