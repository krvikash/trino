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
import io.trino.plugin.warp.gen.stats.TestStats;
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
import java.util.Optional;
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

    public void init(QueryArgs queryArgs)
    {
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()] = INVALID_FILE_COOKIE_FD;
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()] = storageEngine.fileOpen(queryArgs.queryParams().getFilePath());
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()] = StorageUtils.fileHash64(queryArgs.queryParams().getFilePath());
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()] = queryArgs.queryParams().getFileModTime();
    }

    public CollectOpenResult open(QueryArgs queryArgs, StorageCollectorArgs storageCollectorArgs, int numRowsCollectedInPrevRounds, int rowsLimit, Optional<StoreRowListResult> storeRowListResult)
    {
        bufferAllocator.readerOnAllocBundle();
        return collectTxService.collectOpenAndRestore(queryArgs,
                rowsLimit,
                numRowsCollectedInPrevRounds,
                storageCollectorArgs,
                storeRowListResult);
    }

    // returns true if we should stop before this collect since query result type is now single, false otherwise
    boolean prepareChunk(QueryArgs queryArgs, CollectOpenResult collectOpenResult, int numCollectedRows)
    {
        if (chunksQueueService.isChunkPreparationNeeded(queryArgs.chunksQueue())) {
            boolean stopForOptimization = collectTxService.prepareChunk(collectOpenResult.queryMemoryId(),
                    queryArgs.chunksQueue().getCurrent(),
                    collectOpenResult.rowsLimit() - numCollectedRows,
                    queryArgs.chunksQueue().getCurrentResetPoint());
            chunksQueueService.setFirstChunkPrepared(queryArgs.chunksQueue());
            return stopForOptimization && (numCollectedRows > 0);
        }
        return false;
    }

    boolean advanceChunk(QueryArgs queryArgs, CollectOpenResult collectOpenResult, int numCollectedRows)
    {
        if (numCollectedRows == 0 || rangeFillerService.isCurrentChunkCompleted(collectOpenResult.rangeData(), queryArgs.chunkSize())) {
            queryArgs.chunksQueue().currentCompleted();
            logger.debug("collectFromStorage advance numCollectedRows %d", numCollectedRows);
            return true;
        }
        return false;
    }

    // returns true if we should stop after this collect since query result type is different than raw, false otherwise
    boolean collect(CollectOpenResult collectOpenResult,
            int numWes,
            int chunkIndex,
            int numToCollect,
            int[] outQueryResultType)
    {
        return collectTxService.collect(collectOpenResult.queryMemoryId(),
                numWes,
                chunkIndex,
                numToCollect,
                outQueryResultType);
    }

    // returns indication if anything is collected in the buffer and if the buffer is full
    @NativeInterrupt
    CollectFromStorageResult collectFromStorage(QueryArgs queryArgs,
            CollectOpenResult collectOpenResult,
            boolean isMatchGetNumRanges,
            int numCollectedRows,
            int[] outQueryResultType)
    {
        if (chunksQueueService.isCompletelyFinished(queryArgs.chunksQueue(), queryArgs.numChunks())) {
            return new CollectFromStorageResult(CollectBufferState.COLLECT_BUFFER_STATE_EMPTY, numCollectedRows);
        }

        int numToCollect = 1; // Not a real value, just making sure to enter the loop in the first iteration
        boolean stopForOptimization = false;
        while (!chunksQueueService.isChunkRangeCompleted(queryArgs.chunksQueue()) && numToCollect > 0) {
            // get next chunk to collect and check if its already done on buffer
            int chunkIndex = queryArgs.chunksQueue().getCurrent();
            if (prepareChunk(queryArgs, collectOpenResult, numCollectedRows)) {
                numToCollect = 0;
                break;
            }

            QueryParams queryParams = queryArgs.queryParams();
            if (queryParams.getNumCollectElements() > 0) {
                int numCollectedFromCurrentChunk = rangeFillerService.getNumCollectedFromCurrentChunk(chunkIndex, collectOpenResult.rangeData());
                numToCollect = getNumToCollect(queryArgs, numCollectedFromCurrentChunk, collectOpenResult, numCollectedRows);
                if (numToCollect > 0) {
                    stopForOptimization = collect(collectOpenResult, queryParams.getNumCollectElements(), chunkIndex, numToCollect, outQueryResultType);
                }
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, queryArgs, isMatchGetNumRanges, collectOpenResult, this);
            }
            else {
                numCollectedRows += rangeFillerService.add(chunkIndex, 0, queryArgs, isMatchGetNumRanges, collectOpenResult, this);
            }
            logger.debug("collectFromStorage after native collect chunkIndex %d numToCollect %d numCollectedRows %d", chunkIndex, numToCollect, numCollectedRows);

            if (!advanceChunk(queryArgs, collectOpenResult, numCollectedRows)) {
                numToCollect = 0; // We do not collect from one chunk twice in one round
            }
            // In case we are in full scan we are stopping after one chunk
            if ((queryParams.getNumMatchElements() == 0) || stopForOptimization) {
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
            QueryArgs queryArgs,
            StorageCollectorArgs storageCollectorArgs,
            int rowsToFill,
            int numRowsCollectedInPrevRounds,
            int[] queryResultTypes,
            DispatcherPageSourceStats stats,
            TestStats testStats)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = queryArgs.queryParams().getCollectElementsParamsList();

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

    int getNumToCollect(QueryArgs queryArgs,
            int numCollectedFromCurrentChunk,
            CollectOpenResult collectOpenResult,
            int numCollectedRows)
    {
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = collectOpenResult.warmupElementRecordBufferStates();
        if (warmupElementRecordBufferStates.isEmpty()) {
            logger.debug("getNumToCollect no wes %d", queryArgs.chunkSize() - numCollectedRows);
            return queryArgs.chunkSize() - numCollectedRows;
        }

        int recLimit = Integer.MAX_VALUE;
        for (WarmupElementRecordBufferState warmupElementRecordBufferState : warmupElementRecordBufferStates) {
            int limit = getNumToCollect(queryArgs,
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

    int getNumToCollect(QueryArgs queryArgs,
            int numCollectedFromCurrentChunk,
            WarmupElementRecordBufferState warmupElementRecordBufferState,
            RangeData rangeData,
            int numCollectedRows)
    {
        IntBuffer recordBufferStateBuff = bufferAllocator.ids2RecordBufferStateBuff(warmupElementRecordBufferState.getRecordBufferStateBuffId());
        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());
        int maxToCollect = queryArgs.chunkSize() - numCollectedRows; // according to buffer capacity
        int numToCollect = getTotalNumToCollect(queryArgs, rowsBuff) - numCollectedFromCurrentChunk; // according to current chunk
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
    private int getTotalNumToCollect(QueryArgs queryArgs, ShortBuffer rowsBuff)
    {
        int total = Short.toUnsignedInt(rowsBuff.get(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TOTAL_SIZE.ordinal()));
        return (total > 0) ? total : queryArgs.chunkSize();
    }

    public QueryArgs getQueryArgs(QueryParams queryParams)
    {
        TxArgs txArgs = getTxArgs(queryParams);
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }
        int numChunksInRange = getNumChunksInRange(queryParams);

        ChunksQueue chunksQueue = new ChunksQueue(numChunksInRange, storageEngineConstants.getPageSize());

        return new QueryArgs(queryParams,
                txArgs,
                chunkSize,
                numChunks,
                numChunksInRange,
                chunksQueue);
    }

    TxArgs getTxArgs(QueryParams queryParams)
    {
        int[] weCollectParams = queryParams.dumpCollectParams();
        long[][] collectBuffIds = new long[queryParams.getNumCollectElements()][];
        for (int collectIx = 0; collectIx < queryParams.getNumCollectElements(); collectIx++) {
            collectBuffIds[collectIx] = bufferAllocator.getQueryIdsArray();
        }

        byte[] collectStoreBuff = new byte[(int) storageEngine.queryGetCollectStateSize(queryParams.getNumMatchCollect())];
        byte[] collect2MatchParams = new byte[storageEngine.queryGetCollect2MatchSize()];

        // file is opened at init
        long[] fileCookieParams = new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()];

        return new TxArgs(
                weCollectParams,
                collectBuffIds,
                collectStoreBuff,
                collect2MatchParams,
                fileCookieParams);
    }

    StorageCollectorArgs getStorageCollectorArgs(QueryArgs queryArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();
        StorageCollectorCallBack storageCollectorCallBack = new StorageCollectorCallBack(queryArgs.txArgs(), bufferAllocator);

        ArrayList<BlockFiller<?>> blockFillers = new ArrayList<>(queryParams.getNumCollectElements());
        for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
            blockFillers.add(blockFillersFactory.getBlockFiller(collectParams.getBlockRecTypeCode().ordinal()));
        }
        List<ReadJuffersWarmUpElement> collectJuffersWE = queryParams.getCollectElementsParamsList()
                .stream()
                .map(we -> new ReadJuffersWarmUpElement(bufferAllocator, true))
                .collect(Collectors.toList());
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        byte[] storeRowListBuff = new byte[(chunkSize + RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal()) * Short.BYTES];

        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }

        return new StorageCollectorArgs(
                storageCollectorCallBack,
                blockFillers,
                collectJuffersWE,
                storeRowListBuff);
    }

    private int getNumChunksInRange(QueryParams queryParams)
    {
        int numChunksInRange = storageEngineConstants.getMaxChunksInRange();
        if (queryParams.getNumMatchElements() == 0) {
            return numChunksInRange;
        }

        int numLucene = queryParams.getNumLucene();
        int numLuceneLimit = storageEngineConstants.getMaxChunksInRange();
        while ((numChunksInRange > 1) && (numLucene > numLuceneLimit)) {
            numLuceneLimit <<= 1;
            numChunksInRange >>= 1;
        }
        return numChunksInRange;
    }

    public int getMinForTypeAll(int baseRow, CollectOpenResult collectOpenResult, QueryArgs queryArgs, int currentNumCollectedRows)
    {
        return rangeFillerService.getMinForTypeAll(baseRow, collectOpenResult, currentNumCollectedRows);
    }

    public WarpStoragePageSource.RowRanges collectRanges(CollectOpenResult collectOpenResult)
    {
        return rangeFillerService.collectRanges(collectOpenResult.rangeData(), collectOpenResult.rowsLimit());
    }

    public CollectCloseResult close(QueryArgs queryArgs,
            CollectOpenResult collectOpenResult,
            StorageCollectorArgs storageCollectorArgs,
            int numRowsCollectedInCurRound,
            TestStats testStats)
    {
        CollectCloseResult collectCloseResult = collectTxService.collectStoreAndClose(queryArgs,
                collectOpenResult,
                storageCollectorArgs,
                numRowsCollectedInCurRound,
                testStats);

        bufferAllocator.readerOnFreeBundle();
        return collectCloseResult;
    }

    public void abort(CollectOpenResult collectOpenResult, Exception e)
    {
        collectTxService.collectAbort(collectOpenResult, e);
        bufferAllocator.readerOnFreeBundle();
    }

    public void terminate(QueryArgs queryArgs)
    {
        storageEngine.fileClose((int) queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()]);
    }

    public void cleanStorageCache()
    {
        storageEngine.cleanStorageCache();
    }
}
