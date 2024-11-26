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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecordIndexListHeader;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
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

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory.STATS_NATIVE_KEY;
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
    protected final StorageEngine storageEngine;
    protected final BufferAllocator bufferAllocator;
    protected final DictionaryStats dictionaryStats;
    private final RangeFillerService rangeFillerService;
    private final ChunksQueueService chunksQueueService;
    private final CollectTxService collectTxService;
    private final StorageEngineConstants storageEngineConstants;
    private final BlockFillersFactory blockFillersFactory;
    private final DictionaryCacheService dictionaryCacheService;
    private final ShapingLogger shapingLogger;

    @Inject
    StorageCollectorService(
            StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            CollectTxService collectTxService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory,
            DictionaryCacheService dictionaryCacheService,
            GlobalConfig globalConfig)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.collectTxService = requireNonNull(collectTxService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create(DICTIONARY_STAT_GROUP));
        this.rangeFillerService = requireNonNull(rangeFillerService);
        this.chunksQueueService = requireNonNull(chunksQueueService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.blockFillersFactory = requireNonNull(blockFillersFactory);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    private void loadDictionaries(QueryArgs queryArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();

        if (queryParams.getNumLoadDataValues() == 0) {
            return;
        }

        try {
            for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
                // load dictionaries if needed according to existence of dictionary key prepared earlier
                if (collectParams.hasDictionaryParams()) {
                    collectParams.setDictionary(dictionaryCacheService.computeReadIfAbsent(
                            collectParams.getDictionaryKey(),
                            collectParams.getUsedDictionarySize(),
                            collectParams.getDataValuesRecTypeCode(),
                            collectParams.getRecTypeLength(),
                            collectParams.getDictionaryOffset(),
                            queryParams.getFilePath()));
                    dictionaryStats.incdictionary_read_elements_count();
                }
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "loadDictionaries failed");
            throw e;
        }
    }

    private void fileOpen(QueryArgs queryArgs)
    {
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()] = INVALID_FILE_COOKIE_FD;
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()] = storageEngine.fileOpen(queryArgs.queryParams().getFilePath());
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()] = StorageUtils.fileHash64(queryArgs.queryParams().getFilePath());
        queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()] = queryArgs.queryParams().getFileModTime();
    }

    public void init(QueryArgs queryArgs)
    {
        fileOpen(queryArgs);
        loadDictionaries(queryArgs);
    }

    public CollectOpenResult open(QueryArgs queryArgs,
            StorageCollectorArgs storageCollectorArgs,
            WarpQueryState queryState,
            int rowsLimit)
    {
        bufferAllocator.readerOnAllocBundle();
        queryState.resetNumRecordsInCurPage();
        return collectTxService.collectOpenAndRestore(queryArgs,
                rowsLimit,
                queryState.getTotalNumReadRecords(),
                storageCollectorArgs,
                queryState.getStoreRowListResult(),
                queryState.getChunksWithStoredBitmaps());
    }

    void prepareChunk(QueryArgs queryArgs, CollectOpenResult collectOpenResult, int numCollectedRows, MemorySegment outQueryResultTypes)
    {
        collectTxService.prepareChunk(collectOpenResult.queryMemoryId(),
                queryArgs.chunksQueue().getCurrent(),
                collectOpenResult.rowsLimit() - numCollectedRows,
                queryArgs.chunksQueue().getCurrentResetPoint(),
                outQueryResultTypes,
                queryArgs.dispatcherPageSourceStats());
        chunksQueueService.setFirstChunkPrepared(queryArgs.chunksQueue());
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

    private boolean isSingle(QueryResultType queryResultType)
    {
        return queryResultType == QueryResultType.QUERY_RESULT_TYPE_SINGLE ||
                queryResultType == QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL ||
                queryResultType == QueryResultType.QUERY_RESULT_TYPE_ALL_NULL;
    }

    boolean stopForOptimization(int numWes, MemorySegment currQueryResultTypes, MemorySegment prevQueryResultTypes)
    {
        for (int weIx = 0; weIx < numWes; weIx++) {
            QueryResultType currResultType = QueryResultType.values()[currQueryResultTypes.getAtIndex(ValueLayout.JAVA_INT, weIx)];
            QueryResultType prevResultType = QueryResultType.values()[prevQueryResultTypes.getAtIndex(ValueLayout.JAVA_INT, weIx)];
            if (isSingle(currResultType) || isSingle(prevResultType)) {
                return true;
            }
        }
        return false;
    }

    void collectChunk(CollectOpenResult collectOpenResult,
            int numWes,
            int chunkIndex,
            int numToCollect,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectTxService.collectChunk(collectOpenResult.queryMemoryId(),
                numWes,
                chunkIndex,
                numToCollect,
                outQueryResultTypes,
                dispatcherPageSourceStats);
    }

    // returns indication if we can continue preparing more records, or we reached some limit by the storage collector
    @NativeInterrupt
    boolean prepareBlocks(QueryArgs queryArgs,
            StorageCollectorArgs storageCollectorArgs,
            CollectOpenResult collectOpenResult,
            WarpQueryState queryState)
    {
        int numCollectedRows = queryState.getNumRecordsInCurPage();

        if (chunksQueueService.isCompletelyFinished(queryArgs.chunksQueue(), queryArgs.numChunks())) {
            return false;
        }

        boolean canPrepareMore = true;
        QueryParams queryParams = queryArgs.queryParams();

        while (!chunksQueueService.isChunkRangeCompleted(queryArgs.chunksQueue()) && canPrepareMore) {
            // get next chunk to collect and check if its already done on buffer
            int chunkIndex = queryArgs.chunksQueue().getCurrent();
            if (chunksQueueService.isChunkPreparationNeeded(queryArgs.chunksQueue())) {
                prepareChunk(queryArgs, collectOpenResult, numCollectedRows, storageCollectorArgs.prepareQueryResultTypes());
                if ((numCollectedRows > 0) && stopForOptimization(queryParams.getNumCollectElements(), storageCollectorArgs.prepareQueryResultTypes(), storageCollectorArgs.queryResultTypes())) {
                    canPrepareMore = false;
                    break;
                }
            }
            if (queryParams.getNumCollectElements() > 0) {
                int numCollectedFromCurrentChunk = rangeFillerService.getNumCollectedFromCurrentChunk(chunkIndex, collectOpenResult.rangeData());
                int numToCollect = getNumToCollect(queryArgs, numCollectedFromCurrentChunk, collectOpenResult, numCollectedRows);
                if (numToCollect > 0) {
                    collectChunk(
                            collectOpenResult,
                            queryParams.getNumCollectElements(),
                            chunkIndex,
                            numToCollect,
                            storageCollectorArgs.queryResultTypes(),
                            queryArgs.dispatcherPageSourceStats());
                }
                else {
                    canPrepareMore = false;
                }
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, queryArgs, collectOpenResult, this);
            }
            else {
                numCollectedRows += rangeFillerService.add(chunkIndex, 0, queryArgs, collectOpenResult, this);
            }
            logger.debug("collectFromStorage after native collect chunkIndex %d canPrepareMore %b numCollectedRows %d", chunkIndex, canPrepareMore, numCollectedRows);

            if (!advanceChunk(queryArgs, collectOpenResult, numCollectedRows)) {
                canPrepareMore = false; // We do not collect from one chunk twice in one round
            }

            // In case we are in full scan we are stopping after one chunk
            if (queryParams.getNumMatchElements() == 0) {
                canPrepareMore = false;
            }
        }

        logger.debug("collectFromStorage end numCollectedRows %d canPrepareMore %b", numCollectedRows, canPrepareMore);

        queryState.setNumRecordsInCurPage(numCollectedRows);
        return canPrepareMore;
    }

    void fillBlocks(Block[] blocks,
            QueryArgs queryArgs,
            StorageCollectorArgs storageCollectorArgs,
            WarpQueryState queryState)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = queryArgs.queryParams().getCollectElementsParamsList();
        int rowsToFill = queryState.getNumRecordsInCurPage();

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            QueryResultType queryResultType = QueryResultType.values()[storageCollectorArgs.queryResultTypes().getAtIndex(ValueLayout.JAVA_INT, weIx)];
            BlockFiller<?> blockFiller = storageCollectorArgs.blockFillers().get(weIx);
            ReadJuffersWarmUpElement readJuffersWarmUpElement = storageCollectorArgs.collectJuffersWE().get(weIx);
            Block block = blockFiller.fillBlockWithRecords(collectParams, readJuffersWarmUpElement, rowsToFill, queryResultType, dictionaryStats, queryArgs.dispatcherPageSourceStats());
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, () -> {
                queryArgs.dispatcherPageSourceStats().incwrapped_collect_loaded_lazy_blocks();
                return block;
            });
        }
        queryArgs.dispatcherPageSourceStats().addwrapped_collect_total_lazy_blocks(collectElementsParamsList.size());
        queryArgs.dispatcherPageSourceStats().addcached_read_rows(rowsToFill);
    }

    int getNumToCollect(QueryArgs queryArgs,
            int numCollectedFromCurrentChunk,
            CollectOpenResult collectOpenResult,
            int numCollectedRows)
    {
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = collectOpenResult.warmupElementRecordBufferStates();
        if (warmupElementRecordBufferStates.isEmpty()) {
            logger.debug("getNumToCollect no wes %d", queryArgs.chunkSize() - numCollectedRows);
            return Math.min(queryArgs.chunkSize() - numCollectedRows, collectOpenResult.rowsLimit() - numCollectedRows);
        }

        int recLimit = collectOpenResult.rowsLimit() - numCollectedRows;
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

    int getFreeBytes(WarmupElementRecordBufferState warmupElementRecordBufferState)
    {
        return warmupElementRecordBufferState.getFreeBytes();
    }

    int getNumToCollect(QueryArgs queryArgs,
            int numCollectedFromCurrentChunk,
            WarmupElementRecordBufferState warmupElementRecordBufferState,
            RangeData rangeData,
            int numCollectedRows)
    {
        int maxToCollect = queryArgs.chunkSize() - numCollectedRows; // according to buffer capacity
        int numToCollect = getTotalNumToCollect(queryArgs, rangeData) - numCollectedFromCurrentChunk; // according to current chunk
        if (numToCollect > maxToCollect) {
            logger.debug("getNumToCollect zero numToCollect %d maxToCollect %d", numToCollect, maxToCollect);
            return 0; // we want to avoid decompressing twice the same chunk
        }

        int maxRecordLength = warmupElementRecordBufferState.getMaxRecordLength();
        if (maxRecordLength <= storageEngineConstants.getFixedLengthStringLimit()) {
            logger.debug("getNumToCollect fixed size numToCollect %d maxToCollect %d", numToCollect, maxToCollect);
            return numToCollect;
        }
        int freeBytes = getFreeBytes(warmupElementRecordBufferState);

        int actualNumToCollect = Math.min(freeBytes / maxRecordLength, numToCollect);
        logger.debug("getNumToCollect var size actualNumToCollect %d numToCollect %d maxToCollect %d maxRecordLength %d freeBytes %d",
                actualNumToCollect, numToCollect, maxToCollect, maxRecordLength, freeBytes);
        return actualNumToCollect;
    }

    // since the size is a short, zero means a full chunk, we translate to integer here
    private int getTotalNumToCollect(QueryArgs queryArgs, RangeData rangeData)
    {
        int total = rangeData.getRecordIndexes().getSize();
        return (total > 0) ? total : queryArgs.chunkSize();
    }

    public QueryArgs getQueryArgs(QueryParams queryParams, CustomStatsContext customStatsContext)
    {
        TxArgs txArgs = getTxArgs(queryParams);
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);
        NativeStats nativeStats = (NativeStats) customStatsContext.getStat(STATS_NATIVE_KEY);

        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }
        int numChunksInRange = getNumChunksInRange(queryParams);

        ChunksQueue chunksQueue = new ChunksQueue(numChunksInRange, storageEngineConstants.getPageSize());

        Arena arena = Arena.ofAuto();
        Optional<byte[]> storeMatchCollectMetadataBuff = Optional.empty();
        Optional<SequenceLayout> matchCollectMetadataLayout = Optional.empty();
        if (queryParams.getNumMatchCollect() > 0) {
            storeMatchCollectMetadataBuff = Optional.of(new byte[storageEngineConstants.getMatchCollectBufferSize() * queryParams.getNumMatchCollect()]);
            matchCollectMetadataLayout = Optional.of(MemoryLayout.sequenceLayout(queryParams.getNumMatchCollect(), MemoryLayout.paddingLayout(storageEngineConstants.getMatchCollectBufferSize())));
        }

        return new QueryArgs(queryParams,
                dispatcherPageSourceStats,
                nativeStats,
                txArgs,
                chunkSize,
                numChunks,
                numChunksInRange,
                chunksQueue,
                storeMatchCollectMetadataBuff,
                matchCollectMetadataLayout.map(m -> arena.allocate(m.byteSize(), ValueLayout.JAVA_INT.byteSize())),
                arena);
    }

    TxArgs getTxArgs(QueryParams queryParams)
    {
        int[] weCollectParams = queryParams.dumpCollectParams();
        long[][] collectBuffers = new long[queryParams.getNumCollectElements()][];
        for (int collectIx = 0; collectIx < queryParams.getNumCollectElements(); collectIx++) {
            collectBuffers[collectIx] = bufferAllocator.getCollectBuffersArray();
        }

        byte[] collectStoreBuff = new byte[storageEngineConstants.getPageSize() * storageEngineConstants.getMaxChunksInRange()];
        // file is opened at init
        long[] fileCookieParams = new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()];

        return new TxArgs(
                weCollectParams,
                collectBuffers,
                collectStoreBuff,
                fileCookieParams);
    }

    StorageCollectorArgs getStorageCollectorArgs(QueryArgs queryArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();

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

        Arena arena = queryArgs.arena();
        SequenceLayout recordBufferStatesLayout =
                MemoryLayout.sequenceLayout(queryParams.getNumCollectElements(), WarmupElementRecordBufferState.RECORD_BUFFER_STATE_LAYOUT);
        SequenceLayout queryResultTypesLayout =
                MemoryLayout.sequenceLayout(queryParams.getNumCollectElements(), ValueLayout.JAVA_INT);
        SequenceLayout warmUpElementAttsLayout =
                MemoryLayout.sequenceLayout(queryParams.getNumCollectElements(), WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT);

        return new StorageCollectorArgs(blockFillers,
                collectJuffersWE,
                storeRowListBuff,
                arena.allocate(recordBufferStatesLayout.byteSize(), ValueLayout.JAVA_INT.byteSize()),
                new RecordIndexes(arena.allocate(RecordIndexes.RECORD_INDEXES_LAYOUT.byteSize(), ValueLayout.JAVA_SHORT.byteSize())),
                arena.allocate(queryResultTypesLayout.byteSize(), ValueLayout.JAVA_INT.byteSize()),
                arena.allocate(queryResultTypesLayout.byteSize(), ValueLayout.JAVA_INT.byteSize()),
                arena.allocate(warmUpElementAttsLayout.byteSize(), ValueLayout.JAVA_BYTE.byteSize()));
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

    public long close(QueryArgs queryArgs,
            CollectOpenResult collectOpenResult,
            StorageCollectorArgs storageCollectorArgs,
            WarpQueryState queryState)
    {
        CollectCloseResult collectCloseResult = collectTxService.collectStoreAndClose(queryArgs,
                collectOpenResult,
                storageCollectorArgs);

        queryState.setStoreRowListResult(collectCloseResult.storeRowListResult());
        queryState.setChunksWithStoredBitmaps(collectCloseResult.chunksWithStoredBitmaps());

        bufferAllocator.readerOnFreeBundle();
        return collectCloseResult.readPages();
    }

    public void abort(CollectOpenResult collectOpenResult, Exception e, DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectTxService.collectAbort(collectOpenResult, e, dispatcherPageSourceStats);
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
