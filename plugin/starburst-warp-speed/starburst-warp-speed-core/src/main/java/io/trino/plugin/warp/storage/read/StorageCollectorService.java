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
import io.airlift.log.Logger;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecordBufferState;
import io.trino.plugin.warp.gen.constants.RecordIndexListHeader;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.read.fill.BlockFiller;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.plugin.warp.util.StorageUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_NUM_OF;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageCollectorService
{
    private static final Logger logger = Logger.get(StorageCollectorService.class);

    // services
    final StorageEngine storageEngine;
    private final CollectTxService collectTxService;
    final BufferAllocator bufferAllocator;
    final DictionaryStats dictionaryStats;
    final RangeFillerService rangeFillerService;
    final ChunksQueueService chunksQueueService;
    private final StorageEngineConstants storageEngineConstants;
    private final BlockFillersFactory blockFillersFactory;

    @Inject
    StorageCollectorService(
            StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            CollectTxService collectTxService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.collectTxService = requireNonNull(collectTxService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create(DICTIONARY_STAT_GROUP));
        this.rangeFillerService = requireNonNull(rangeFillerService);
        this.chunksQueueService = requireNonNull(chunksQueueService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.blockFillersFactory = requireNonNull(blockFillersFactory);
    }

    void prepareChunk(StorageCollectorArgs storageCollectorArgs, CollectOpenResult collectOpenResult, int numCollectedRows)
    {
        if (chunksQueueService.isChunkPreparationNeeded(storageCollectorArgs.chunksQueue())) {
            collectTxService.prepareChunk(collectOpenResult.collectTxId(),
                    storageCollectorArgs.chunksQueue().getCurrent(),
                    collectOpenResult.rowsLimit() - numCollectedRows,
                    storageCollectorArgs.chunksQueue().getCurrentResetPoint());
            chunksQueueService.setFirstChunkPrepared(storageCollectorArgs.chunksQueue());
        }
    }

    boolean advanceChunk(StorageCollectorArgs storageCollectorArgs, CollectOpenResult collectOpenResult, int numCollectedRows)
    {
        if (numCollectedRows == 0 || rangeFillerService.isCurrentChunkCompleted(collectOpenResult.rangeData(), storageCollectorArgs.chunkSize())) {
            storageCollectorArgs.chunksQueue().currentCompleted();
            logger.debug("collectFromStorage advance numCollectedRows %d", numCollectedRows);
            return true;
        }
        return false;
    }

    void collect(CollectOpenResult collectOpenResult, int numWes, int chunkIndex, int numToCollect, int[] outQueryResultType)
    {
        collectTxService.collect(collectOpenResult.collectTxId(), numWes, chunkIndex, numToCollect, outQueryResultType);
    }

    // returns indication if anything is collected in the buffer and if the buffer is full
    @NativeInterrupt
    CollectFromStorageResult collectFromStorage(CollectOpenResult collectOpenResult,
            StorageCollectorArgs storageCollectorArgs,
            boolean isMatchGetNumRanges,
            int numCollectedRows,
            int[] queryResultType)
    {
        if (chunksQueueService.isCompletelyFinished(storageCollectorArgs.chunksQueue(), storageCollectorArgs.numChunks())) {
            return new CollectFromStorageResult(CollectBufferState.COLLECT_BUFFER_STATE_EMPTY, numCollectedRows);
        }

        int numToCollect = 1; // Not a real value, just making sure to enter the loop in the first iteration
        while (!chunksQueueService.isChunkRangeCompleted(storageCollectorArgs.chunksQueue()) && numToCollect > 0) {
            // get next chunk to collect and check if its already done on buffer
            int chunkIndex = storageCollectorArgs.chunksQueue().getCurrent();
            prepareChunk(storageCollectorArgs, collectOpenResult, numCollectedRows);

            QueryParams queryParams = storageCollectorArgs.collectTxArgs().queryParams();
            if (queryParams.getNumCollectElements() > 0) {
                int numCollectedFromCurrentChunk = rangeFillerService.getNumCollectedFromCurrentChunk(chunkIndex, collectOpenResult.rangeData());
                numToCollect = getNumToCollect(storageCollectorArgs, numCollectedFromCurrentChunk, collectOpenResult, numCollectedRows);
                if (numToCollect > 0) {
                    collect(collectOpenResult, queryParams.getNumCollectElements(), chunkIndex, numToCollect, queryResultType);
                }
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, storageCollectorArgs, isMatchGetNumRanges, collectOpenResult, this);
            }
            else {
                numCollectedRows += rangeFillerService.add(chunkIndex, 0, storageCollectorArgs, isMatchGetNumRanges, collectOpenResult, this);
            }
            logger.debug("collectFromStorage after native collect chunkIndex %d numToCollect %d numCollectedRows %d", chunkIndex, numToCollect, numCollectedRows);

            if (!advanceChunk(storageCollectorArgs, collectOpenResult, numCollectedRows)) {
                numToCollect = 0; // We do not collect from one chunk twice in one round
            }
            // In case we are in full scan we are stopping after one chunk
            if (queryParams.getNumMatchElements() == 0) {
                numToCollect = 0;
            }
        }

        logger.debug("collectFromStorage end numCollectedRows %d numToCollect %d", numCollectedRows, numToCollect);
        CollectBufferState collectBufferState;
        if (numToCollect == 0) {
            collectBufferState = CollectBufferState.COLLECT_BUFFER_STATE_FULL;
        }
        else {
            collectBufferState = (numCollectedRows > 0) ? CollectBufferState.COLLECT_BUFFER_STATE_PARTIAL : CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
        }

        return new CollectFromStorageResult(collectBufferState, numCollectedRows);
    }

    void fillBlocks(Block[] blocks,
            StorageCollectorArgs storageCollectorArgs,
            int rowsToFill,
            int numRowsCollectedInPrevRounds,
            int[] queryResultTypes,
            DispatcherPageSourceStats stats)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = storageCollectorArgs.collectTxArgs().queryParams().getCollectElementsParamsList();

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            QueryResultType queryResultType = QueryResultType.values()[queryResultTypes[weIx]];
            BlockFiller<?> blockFiller = storageCollectorArgs.blockFillers().get(weIx);
            ReadJuffersWarmUpElement readJuffersWarmUpElement = storageCollectorArgs.collectJuffersWE().get(weIx);
            Block block = blockFiller.fillBlockWithRecords(collectParams, readJuffersWarmUpElement, rowsToFill, queryResultType, dictionaryStats);
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, () -> {
                stats.incwrapped_collect_loaded_lazy_blocks();
                return block;
            });
        }
        stats.addwrapped_collect_total_lazy_blocks(collectElementsParamsList.size());
    }

    int getNumToCollect(StorageCollectorArgs storageCollectorArgs,
            int numCollectedFromCurrentChunk,
            CollectOpenResult collectOpenResult,
            int numCollectedRows)
    {
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = collectOpenResult.warmupElementRecordBufferStates();
        if (warmupElementRecordBufferStates.isEmpty()) {
            logger.debug("getNumToCollect no wes %d", storageCollectorArgs.chunkSize() - numCollectedRows);
            return storageCollectorArgs.chunkSize() - numCollectedRows;
        }

        int recLimit = Integer.MAX_VALUE;
        for (WarmupElementRecordBufferState warmupElementRecordBufferState : warmupElementRecordBufferStates) {
            int limit = getNumToCollect(storageCollectorArgs,
                    numCollectedFromCurrentChunk,
                    warmupElementRecordBufferState,
                    collectOpenResult.rangeData(),
                    numCollectedRows);
            if (limit < recLimit) {
                recLimit = limit;
            }
        }
        logger.debug("getNumToCollect numCollectedFromCurrentChunk %d recLimit %d", numCollectedFromCurrentChunk, recLimit);
        return recLimit;
    }

    int getFreeBytes(WarmupElementRecordBufferState warmupElementRecordBufferState, IntBuffer recordBufferStateBuff)
    {
        return recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_TOTAL_BYTES.ordinal()) - recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_USED_BYTES.ordinal());
    }

    int getNumToCollect(StorageCollectorArgs storageCollectorArgs,
            int numCollectedFromCurrentChunk,
            WarmupElementRecordBufferState warmupElementRecordBufferState,
            RangeData rangeData,
            int numCollectedRows)
    {
        IntBuffer recordBufferStateBuff = bufferAllocator.ids2RecordBufferStateBuff(warmupElementRecordBufferState.getRecordBufferStateBuffId());
        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());
        int maxToCollect = storageCollectorArgs.chunkSize() - numCollectedRows; // according to buffer capacity
        int numToCollect = getTotalNumToCollect(storageCollectorArgs, rowsBuff) - numCollectedFromCurrentChunk; // according to current chunk
        if (numToCollect > maxToCollect) {
            logger.debug("getNumToCollect zero basePos %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), numToCollect, maxToCollect);
            return 0; // we want to avoid decompressing twice the same chunk
        }

        int maxRecordLength = recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_MAX_RECORD_LENGTH.ordinal());
        if (maxRecordLength <= storageEngineConstants.getFixedLengthStringLimit()) {
            logger.debug("getNumToCollect fixed size basePos %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), numToCollect, maxToCollect);
            return numToCollect;
        }
        int freeBytes = getFreeBytes(warmupElementRecordBufferState, recordBufferStateBuff);

        int actualNumToCollect = Math.min(freeBytes / maxRecordLength, numToCollect);
        logger.debug("getNumToCollect var size basePos %d actualNumToCollect %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), actualNumToCollect, numToCollect, maxToCollect);
        return actualNumToCollect;
    }

    // since the size is a short, zero means a full chunk, we translate to integer here
    private int getTotalNumToCollect(StorageCollectorArgs storageCollectorArgs, ShortBuffer rowsBuff)
    {
        int total = Short.toUnsignedInt(rowsBuff.get(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TOTAL_SIZE.ordinal()));
        return (total > 0) ? total : storageCollectorArgs.chunkSize();
    }

    CollectTxArgs getCollectTxArgs(QueryParams queryParams)
    {
        int[] weCollectParams = queryParams.dumpCollectParams();
        long[][] collectBuffIds = new long[queryParams.getNumCollectElements()][];
        for (int collectIx = 0; collectIx < queryParams.getNumCollectElements(); collectIx++) {
            collectBuffIds[collectIx] = bufferAllocator.getQueryIdsArray(false);
        }

        byte[] collectStoreBuff = new byte[(int) storageEngine.queryGetCollectStateSize(queryParams.getNumMatchCollect())];
        byte[] collect2MatchParams = new byte[storageEngine.queryGetCollect2MatchSize()];

        //  file
        long[] fileCookieParams = new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()];
        fileCookieParams[FILE_COOKIE_PARAMS_FD.ordinal()] = INVALID_FILE_COOKIE_FD;
        fileCookieParams[FILE_COOKIE_PARAMS_FD.ordinal()] = storageEngine.fileOpen(queryParams.getFilePath());
        fileCookieParams[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()] = StorageUtils.fileHash64(queryParams.getFilePath());
        fileCookieParams[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()] = queryParams.getFileModTime();

        return new CollectTxArgs(
                weCollectParams,
                collectBuffIds,
                collectStoreBuff,
                collect2MatchParams,
                queryParams,
                fileCookieParams);
    }

    StorageCollectorArgs getStorageCollectorArgs(QueryParams queryParams)
    {
        CollectTxArgs collectTxArgs = getCollectTxArgs(queryParams);
        ArrayList<BlockFiller<?>> blockFillers = new ArrayList<>(queryParams.getNumCollectElements());
        for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
            blockFillers.add(blockFillersFactory.getBlockFiller(collectParams.getBlockRecTypeCode().ordinal()));
        }
        List<ReadJuffersWarmUpElement> collectJuffersWE = queryParams.getCollectElementsParamsList()
                .stream()
                .map(we -> new ReadJuffersWarmUpElement(bufferAllocator, true, false))
                .collect(Collectors.toList());
        int numChunksInRange = getNumChunksInRange(queryParams);
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        byte[] storeRowListBuff = new byte[(chunkSize + RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal()) * Short.BYTES];

        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }

        return new StorageCollectorArgs(
                collectTxArgs,
                blockFillers,
                numChunksInRange,
                collectJuffersWE,
                storeRowListBuff,
                chunkSize,
                numChunks,
                new ChunksQueue(numChunksInRange, storageEngineConstants.getPageSize()));
    }

    private int getNumChunksInRange(QueryParams queryParams)
    {
        int numChunksInRange = storageEngineConstants.getMaxChunksInRange();
        if (queryParams.getNumMatchElements() == 0) {
            return numChunksInRange;
        }

        int numLucene = queryParams.getNumLucene();
        int numLuceneLimit = storageEngineConstants.getMaxLuceneColumnsInBundle();
        while ((numChunksInRange > 1) && (numLucene > numLuceneLimit)) {
            numLuceneLimit <<= 1;
            numChunksInRange >>= 1;
        }
        return numChunksInRange;
    }

    public int getMinForTypeAll(int baseRow, CollectOpenResult collectOpenResult, StorageCollectorArgs storageCollectorArgs, int currentNumCollectedRows)
    {
        return rangeFillerService.getMinForTypeAll(baseRow, collectOpenResult, currentNumCollectedRows);
    }
}
