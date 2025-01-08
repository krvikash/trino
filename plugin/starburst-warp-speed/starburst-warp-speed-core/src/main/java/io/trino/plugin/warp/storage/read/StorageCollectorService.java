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
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.read.fill.BlockFiller;
import io.trino.plugin.warp.storage.read.fill.BlockFillersFactory;
import io.trino.plugin.warp.util.StorageUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_NUM_OF;
import static io.trino.plugin.warp.gen.constants.RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageCollectorService
        implements BlocksAggregator
{
    private static final Logger logger = Logger.get(StorageCollectorService.class);

    // services
    protected final StorageEngine storageEngine;
    protected final BufferAllocator bufferAllocator;
    protected final DictionaryStats dictionaryStats;
    private final RangeFillerService rangeFillerService;
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
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create());
        this.rangeFillerService = requireNonNull(rangeFillerService);
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
                            collectParams.getDataValuesRecTypeLength(),
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
        queryArgs.fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()] = storageEngine.fileOpen(queryArgs.queryParams().getFilePath());
        queryArgs.fileCookie()[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()] = StorageUtils.fileHash64(queryArgs.queryParams().getFilePath());
        queryArgs.fileCookie()[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()] = queryArgs.queryParams().getFileModTime();
    }

    public AggregatorArgs open(QueryArgs queryArgs)
    {
        fileOpen(queryArgs);
        loadDictionaries(queryArgs);
        return getStorageCollectorArgs(queryArgs);
    }

    public AggregatorPageArgs openPage(QueryArgs queryArgs,
            ThreadArena pageArena,
            AggregatorArgs aggregatorArgs,
            WarpQueryState queryState,
            int rowsLimit)
    {
        queryState.resetNumRecordsInCurPage();
        return collectTxService.collectOpenAndRestore(queryArgs,
                pageArena,
                rowsLimit,
                queryState.getTotalNumReadRecords(),
                aggregatorArgs,
                queryState.getStoreRowListResult());
    }

    void openChunk(ChunksQueue chunksQueue,
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs)
    {
        collectTxService.openChunk(aggregatorPageArgs.collectState(),
                chunksQueue.getCurrent(),
                aggregatorPageArgs.prepareQueryResultTypes().orElse(MemorySegment.NULL),
                queryArgs.dispatcherPageSourceStats());
    }

    boolean advanceChunk(ChunksQueue chunksQueue,
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs,
            int numCollectedRows)
    {
        if (numCollectedRows == 0 || rangeFillerService.updateStartIxIfNotCompleted(aggregatorPageArgs.rangeData())) {
            chunksQueue.currentCompleted();
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

    boolean stopForOptimization(AggregatorPageArgs aggregatorPageArgs, int numWes)
    {
        if (numWes == 0) {
            return false;
        }

        MemorySegment currQueryResultTypes = aggregatorPageArgs.prepareQueryResultTypes().get();
        MemorySegment prevQueryResultTypes = aggregatorPageArgs.queryResultTypes().get();
        for (int weIx = 0; weIx < numWes; weIx++) {
            QueryResultType currResultType = QueryResultType.values()[currQueryResultTypes.getAtIndex(ValueLayout.JAVA_INT, weIx)];
            QueryResultType prevResultType = QueryResultType.values()[prevQueryResultTypes.getAtIndex(ValueLayout.JAVA_INT, weIx)];
            if (isSingle(currResultType) || isSingle(prevResultType)) {
                return true;
            }
        }
        return false;
    }

    void collectChunk(AggregatorPageArgs aggregatorPageArgs,
            boolean isFullScan,
            int startRecIx,
            int numToCollect,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectTxService.collectChunk(aggregatorPageArgs.collectState(),
                isFullScan,
                startRecIx,
                numToCollect,
                outQueryResultTypes,
                dispatcherPageSourceStats);
    }

    // returns indication if we can continue preparing more records, or we reached some limit by the storage collector
    @NativeInterrupt
    public boolean prepareBlocks(ChunksQueue chunksQueue,
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState)
    {
        int numCollectedRows = queryState.getNumRecordsInCurPage();
        RecordIndexes recordIndexes = aggregatorPageArgs.rangeData().getRecordIndexes();

        if (chunksQueue.isCompletelyFinished(queryArgs.numChunks())) {
            return false;
        }

        boolean canPrepareMore = true;
        QueryParams queryParams = queryArgs.queryParams();

        while (!chunksQueue.isChunkRangeCompleted() && canPrepareMore) {
            // get next chunk to collect and check if its already done on buffer
            int chunkIndex = chunksQueue.getCurrent();
            chunksQueue.loadChunk(recordIndexes, queryArgs.numRecordsInChunk(chunkIndex));
            int numToCollect;

            if (queryParams.getNumCollectElements() > 0) {
                openChunk(chunksQueue, queryArgs, aggregatorPageArgs);
                if ((numCollectedRows > 0) && stopForOptimization(aggregatorPageArgs, queryParams.getNumCollectElements())) {
                    canPrepareMore = false;
                    break;
                }
                int numCollectedFromCurrentChunk = rangeFillerService.getNumCollectedFromCurrentChunk(chunkIndex, aggregatorPageArgs.rangeData());
                numToCollect = getNumToCollect(queryArgs, numCollectedFromCurrentChunk, aggregatorPageArgs, numCollectedRows);
                if (numToCollect > 0) {
                    collectChunk(aggregatorPageArgs,
                            recordIndexes.getType() != RECORD_INDEX_LIST_TYPE_VALUES,
                            recordIndexes.getStart(),
                            numToCollect,
                            aggregatorPageArgs.queryResultTypes().get(),
                            queryArgs.dispatcherPageSourceStats());
                }
                else {
                    canPrepareMore = false;
                }
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, queryArgs, aggregatorPageArgs, this);
            }
            else {
                numToCollect = min(recordIndexes.getSize(), aggregatorPageArgs.rowsLimit() - numCollectedRows);
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, queryArgs, aggregatorPageArgs, this);
            }
            logger.debug("collectFromStorage after native collect chunkIndex %d canPrepareMore %b numCollectedRows %d", chunkIndex, canPrepareMore, numCollectedRows);

            if (!advanceChunk(chunksQueue, queryArgs, aggregatorPageArgs, numCollectedRows)) {
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

    public Block[] aggregateBlocks(QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = queryArgs.queryParams().getCollectElementsParamsList();
        MemorySegment queryResultTypes = aggregatorPageArgs.queryResultTypes().orElse(MemorySegment.NULL);
        int rowsToFill = queryState.getNumRecordsInCurPage();
        Block[] blocks = new Block[collectElementsParamsList.size()];

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            QueryResultType queryResultType = QueryResultType.values()[queryResultTypes.getAtIndex(ValueLayout.JAVA_INT, weIx)];
            BlockFiller<?> blockFiller = aggregatorArgs.blockFillers().get(weIx);
            ReadJuffersWarmUpElement readJuffersWarmUpElement = aggregatorArgs.collectJuffersWE().get(weIx);
            Block block = blockFiller.fillBlockWithRecords(collectParams, readJuffersWarmUpElement, rowsToFill, queryResultType, dictionaryStats, queryArgs.dispatcherPageSourceStats());
            blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, () -> {
                queryArgs.dispatcherPageSourceStats().incwrapped_collect_loaded_lazy_blocks();
                return block;
            });
        }
        queryArgs.dispatcherPageSourceStats().addwrapped_collect_total_lazy_blocks(collectElementsParamsList.size());
        queryArgs.dispatcherPageSourceStats().addcached_read_rows(rowsToFill);
        return blocks;
    }

    int getNumToCollect(QueryArgs queryArgs,
            int numCollectedFromCurrentChunk,
            AggregatorPageArgs aggregatorPageArgs,
            int numCollectedRows)
    {
        int pageLimit = aggregatorPageArgs.rowsLimit() - numCollectedRows;
        int chunkLimit = aggregatorPageArgs.rangeData().getRecordIndexes().getSize() - aggregatorPageArgs.rangeData().getRecordIndexes().getStart();
        int recLimit = min(pageLimit, chunkLimit);

        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = aggregatorPageArgs.warmupElementRecordBufferStates();
        if (warmupElementRecordBufferStates.isEmpty()) {
            logger.debug("getNumToCollect no wes %d", queryArgs.chunkSize() - numCollectedRows);
            return recLimit;
        }

        for (WarmupElementRecordBufferState warmupElementRecordBufferState : warmupElementRecordBufferStates) {
            int limit = getNumToCollect(queryArgs,
                    numCollectedFromCurrentChunk,
                    warmupElementRecordBufferState,
                    aggregatorPageArgs.rangeData(),
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

        int actualNumToCollect = min(freeBytes / maxRecordLength, numToCollect);
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
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        NativeStats nativeStats = (NativeStats) customStatsContext.getStat(NativeStats.createKey());

        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }
        int numChunksInRange = getNumChunksInRange(queryParams);

        Optional<byte[]> storeMatchCollectMetadataBuff = Optional.empty();
        if (queryParams.getNumMatchCollect() > 0) {
            storeMatchCollectMetadataBuff = Optional.of(new byte[storageEngineConstants.getMatchCollectMetadataSize() * queryParams.getNumMatchCollect()]);
        }

        return new QueryArgs(queryParams,
                dispatcherPageSourceStats,
                nativeStats,
                new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()],
                chunkSize,
                numChunks,
                numChunksInRange,
                storeMatchCollectMetadataBuff);
    }

    private AggregatorArgs getStorageCollectorArgs(QueryArgs queryArgs)
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

        return new AggregatorArgs(blockFillers,
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

    public WarpStoragePageSource.RowRanges getRanges(AggregatorPageArgs aggregatorPageArgs)
    {
        return rangeFillerService.collectRanges(aggregatorPageArgs.rangeData(), aggregatorPageArgs.rowsLimit());
    }

    public long closePage(QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState,
            ChunksQueue chunksQueue)
    {
        CollectCloseResult collectCloseResult = collectTxService.collectStoreAndClose(queryArgs,
                aggregatorPageArgs,
                aggregatorArgs,
                chunksQueue);
        queryState.setStoreRowListResult(collectCloseResult.storeRowListResult());
        return collectCloseResult.readPages();
    }

    public void abortPage(QueryArgs queryArgs, AggregatorPageArgs aggregatorPageArgs, Exception e)
    {
        collectTxService.collectAbort(aggregatorPageArgs, e, queryArgs.dispatcherPageSourceStats());
    }

    public void close(QueryArgs queryArgs)
    {
        storageEngine.fileClose((int) queryArgs.fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()]);
    }
}
