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
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
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

public class StorageCollectorService
{
    private static final Logger logger = Logger.get(StorageCollectorService.class);
    public static final int INVALID_LAZY_COLLECT_IX = -1;

    private final int lazyCollectMaxLenSupported;
    // services
    private final StorageEngine storageEngine;
    private final CollectTxService collectTxService;
    private final BufferAllocator bufferAllocator;
    private final DictionaryStats dictionaryStats;
    private final RangeFillerService rangeFillerService;
    private final ChunksQueueService chunksQueueService;
    private final StorageEngineConstants storageEngineConstants;
    private final BlockFillersFactory blockFillersFactory;
    private final LazyCollectTxService lazyCollectTxService;
    private final GlobalConfig globalConfig;

    @Inject
    StorageCollectorService(
            StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            CollectTxService collectTxService,
            LazyCollectTxService lazyCollectTxService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory,
            GlobalConfig globalConfig)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.collectTxService = requireNonNull(collectTxService);
        this.lazyCollectTxService = requireNonNull(lazyCollectTxService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create(DICTIONARY_STAT_GROUP));
        this.rangeFillerService = requireNonNull(rangeFillerService);
        this.chunksQueueService = requireNonNull(chunksQueueService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.blockFillersFactory = requireNonNull(blockFillersFactory);
        this.lazyCollectMaxLenSupported = storageEngineConstants.getFixedLengthStringLimit();
        this.globalConfig = globalConfig;
    }

    // returns indication if anything is collected in the buffer and if the buffer is full
    @NativeInterrupt
    CollectFromStorageResult collectFromStorage(CollectOpenResult collectOpenResult,
            boolean isMatchGetNumRanges,
            boolean chunkPrepared,
            int numCollectedRows,
            StorageCollectorArgs storageCollectorArgs)
    {
        int lazyChunkIndex = INVALID_LAZY_COLLECT_IX;
        if (chunksQueueService.isCompletelyFinished(storageCollectorArgs.chunksQueue(), storageCollectorArgs.numChunks())) {
            return new CollectFromStorageResult(CollectBufferState.COLLECT_BUFFER_STATE_EMPTY, chunkPrepared, numCollectedRows, lazyChunkIndex);
        }

        int numToCollect = 1; // Not a real value, just making sure to enter the loop in the first iteration
        while (!chunksQueueService.isChunkRangeCompleted(storageCollectorArgs.chunksQueue()) && numToCollect > 0) {
            // get next chunk to collect and check if its already done on buffer
            int chunkIndex = storageCollectorArgs.chunksQueue().getCurrent();
            int ret = collectTxService.prepareNextChunk(storageCollectorArgs.chunksQueue().getCurrent(),
                    storageCollectorArgs.chunksQueue().getCurrentResetPoint(),
                    chunkPrepared,
                    collectOpenResult.collectTxId(),
                    collectOpenResult.rowsLimit(),
                    numCollectedRows,
                    collectOpenResult.outResultType());
            boolean bufferIsFull = (ret > 0);
            chunkPrepared = true;
            if (bufferIsFull) { // if returns true we need to stop for query result optimization
                numToCollect = 0;
                break;
            }

            QueryParams queryParams = storageCollectorArgs.collectTxArgs().queryParams();
            if (queryParams.getNumCollectElements() > 0) {
                int numCollectedFromCurrentChunk = rangeFillerService.getNumCollectedFromCurrentChunk(chunkIndex, collectOpenResult.rangeData());
                numToCollect = getNumToCollect(storageCollectorArgs, numCollectedFromCurrentChunk, collectOpenResult, numCollectedRows);
                if (numToCollect > 0 && !storageCollectorArgs.isLazyCollect()) {
                    collectTxService.collect(collectOpenResult.collectTxId(), collectOpenResult.outResultType(), queryParams.getNumCollectElements(), chunkIndex, numToCollect);
                }
                else {
                    lazyChunkIndex = storageCollectorArgs.chunksQueue().getCurrent();
                }
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, storageCollectorArgs, isMatchGetNumRanges, collectOpenResult.rangeData());
            }
            else {
                numCollectedRows += rangeFillerService.add(chunkIndex, 0, storageCollectorArgs, isMatchGetNumRanges, collectOpenResult.rangeData());
            }
            logger.debug("collectFromStorage after native collect chunkIndex %d numToCollect %d numCollectedRows %d", chunkIndex, numToCollect, numCollectedRows);

            boolean currentChunkCompleted = rangeFillerService.isCurrentChunkCompleted(collectOpenResult.rangeData(), storageCollectorArgs.chunkSize());
            if (currentChunkCompleted || numCollectedRows == 0) {
                storageCollectorArgs.chunksQueue().currentCompleted();
                logger.debug("collectFromStorage advance numCollectedRows %d", numCollectedRows);
                chunkPrepared = false;
            }
            else {
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

        return new CollectFromStorageResult(collectBufferState, chunkPrepared, numCollectedRows, lazyChunkIndex);
    }

    private boolean useLazyCollect(QueryParams queryParams)
    {
        if (queryParams.getNumMatchElements() != 0 || !queryParams.isLazyCollectEnabled()) {
            return false;
        }

        return queryParams.getCollectElementsParamsList().stream().allMatch(collectParams -> collectParams.getBlockRecTypeLength() <= lazyCollectMaxLenSupported);
    }

    void fillBlocks(Block[] blocks, StorageCollectorArgs storageCollectorArgs, int rowsToFill, int lazyChunkIx, DispatcherPageSourceStats stats)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = storageCollectorArgs.collectTxArgs().queryParams().getCollectElementsParamsList();

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            LazyCollectorArgs lazyCollectorArgs = getLazyCollectorArgs(storageCollectorArgs, weIx, lazyChunkIx, rowsToFill);
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, new LazyCollectorLoader(
                    lazyCollectTxService,
                    lazyCollectorArgs,
                    dictionaryStats,
                    stats,
                    globalConfig));
        }
        stats.addlazy_collect_total_blocks(collectElementsParamsList.size());
    }

    void fillBlocks(Block[] blocks, StorageCollectorArgs storageCollectorArgs, CollectOpenResult collectOpenResult, int rowsToFill)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = storageCollectorArgs.collectTxArgs().queryParams().getCollectElementsParamsList();

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            QueryResultType queryResultType = QueryResultType.values()[collectOpenResult.outResultType()[weIx]];
            BlockFiller<?> blockFiller = storageCollectorArgs.blockFillers().get(weIx);
            ReadJuffersWarmUpElement readJuffersWarmUpElement = storageCollectorArgs.collectJuffersWE().get(weIx);
            blocks[collectParams.getBlockIndex()] = blockFiller.fillBlockWithRecords(collectParams, readJuffersWarmUpElement, rowsToFill, queryResultType, dictionaryStats);
        }
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

    // @TODO we should consider in the future to move the small calculation to native (back to native) and return the result instead of passing the parameters to java and calculating here
    // currently we are getting 3 parameters here and calculating one result. in the future this might be reduced to 2 parameters only (chunk max reclen and used bytes) and
    // the point of calculation might require extra jni call which is not performance oriented. this is why we currently leave it like it is now.
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
        if (maxRecordLength <= storageCollectorArgs.fixedLengthStringLimit()) {
            logger.debug("getNumToCollect fixed size basePos %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), numToCollect, maxToCollect);
            return numToCollect;
        }
        int freeBytes = recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_TOTAL_BYTES.ordinal()) - recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_USED_BYTES.ordinal());
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
        int numChunksInRange = storageEngineConstants.getMaxChunksInRange();
        int fixedLengthStringLimit = storageEngineConstants.getFixedLengthStringLimit();
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        byte[] storeRowListBuff = new byte[(chunkSize + RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal()) * Short.BYTES];

        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }
        ChunksQueue chunksQueue = new ChunksQueue(numChunksInRange, storageEngineConstants.getPageSize());

        boolean isLazyCollect = useLazyCollect(queryParams);

        return new StorageCollectorArgs(
                collectTxArgs,
                blockFillers,
                numChunksInRange,
                fixedLengthStringLimit,
                collectJuffersWE,
                storeRowListBuff,
                chunkSize,
                numChunks,
                chunksQueue,
                isLazyCollect);
    }

    LazyCollectorArgs getLazyCollectorArgs(StorageCollectorArgs storageCollectorArgs, int weIx, int chunkIndex, int numRows)
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
        return new LazyCollectorArgs(
                collectTxArgs,
                collectParams,
                juffersWE,
                storageCollectorArgs.blockFillers().get(weIx),
                chunkIndex,
                numRows);
    }
}
