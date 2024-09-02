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

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneMatcher;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_TX_ALLOCATION_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_MATCH_FAILED;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static java.util.Objects.requireNonNull;

public class StorageReader
{
    private static final Logger logger = Logger.get(StorageReader.class);
    private static final int INVALID_TX_ID = -1;
    private static final long MATCH_RESULT_MASK = 0x00000000ffffffffL;
    private final ShapingLogger shapingLogger;

    // services
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final DictionaryStats dictionaryStats;
    private final DispatcherPageSourceStats statsDispatcherPageSource;
    private final DictionaryCacheService dictionaryCacheService;
    private final LucenePageCacheStats lucenePageCacheStats;
    private final BufferAllocator bufferAllocator;

    // parameters
    private final QueryParams queryParams;
    private final ReadTimeMeasurement readTimeMeasurement;

    // match
    private final int[] weMatchTree;
    private final long[][] matchBuffIds;
    private final List<ReadJuffersWarmUpElement> matchJuffersWE;
    private final LuceneMatcher[] luceneMatchers;
    private boolean matchExhausted;
    private int matchTxId;
    short[] matchedChunksIndexes;
    int[] matchBitmapResetPoints;

    // collect
    private final StorageCollectorArgs storageCollectorArgs;
    private final CollectTxService collectTxService;
    private final ChunksQueueService chunksQueueService;
    private final StorageCollectorService storageCollectorService;
    private int collectTxId;
    private int numRowsCollectedInCurRound; // num rows collected in this getNextPage
    private int numRowsCollectedInPrevRounds; // num rows collected in all previous getNextPages
    private int[] queryResultType;
    private boolean dictionariesLoaded;
    private RecordIndexListType storeRowListType;
    private int storeRowListSize;
    private CollectOpenResult collectOpenResult;

    StorageReader(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            DictionaryCacheService dictionaryCacheService,
            QueryParams queryParams,
            CustomStatsContext customStatsContext,
            StorageCollectorArgs storageCollectorArgs,
            CollectTxService collectTxService,
            ChunksQueueService chunksQueueService,
            StorageCollectorService storageCollectorService,
            GlobalConfig globalConfig)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.statsDispatcherPageSource = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);
        this.dictionaryStats = (DictionaryStats) customStatsContext.getStat(DictionaryCacheService.DICTIONARY_STAT_GROUP);
        this.lucenePageCacheStats = (LucenePageCacheStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_LUCENE_PAGE_CACHE_KEY);
        this.bufferAllocator = bufferAllocator;

        this.queryParams = queryParams;
        this.matchBuffIds = new long[queryParams.getNumMatchElements()][];
        this.luceneMatchers = new LuceneMatcher[queryParams.getNumLucene()];
        this.storageCollectorArgs = storageCollectorArgs;
        this.collectTxService = collectTxService;
        this.chunksQueueService = chunksQueueService;
        this.queryResultType = new int[queryParams.getNumCollectElements()];
        this.storeRowListSize = 0;
        this.storeRowListType = RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL;
        //  match
        this.matchTxId = INVALID_TX_ID;
        this.collectTxId = INVALID_TX_ID;
        this.matchExhausted = true; // we initialize as true, and at the first call it will be set by calling match

        this.weMatchTree = queryParams.dumpMatchParams();
        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
            matchBuffIds[matchIx] = bufferAllocator.getQueryIdsArray(matchParams.hasLuceneParams());
            matchIx++;
        }
        this.matchJuffersWE = queryParams.getMatchElementsParamsList()
                .stream()
                .map(we -> new ReadJuffersWarmUpElement(bufferAllocator, false, we.hasLuceneParams()))
                .collect(Collectors.toList());

        createLuceneMatchers(globalConfig); // this call must be after creating the matchJuffersWE

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        readTimeMeasurement = new ReadTimeMeasurement();
    }

    void close()
    {
        if (storageCollectorArgs != null) {
            storageEngine.fileClose((int) storageCollectorArgs.collectTxArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()]);
        }
    }

    private void createLuceneMatchers(GlobalConfig globalConfig)
    {
        if (luceneMatchers.length == 0) {
            return;
        }

        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
            if (matchParams.hasLuceneParams()) {
                luceneMatchers[matchParams.getLuceneIx()] = new LuceneMatcher(storageEngine,
                        storageEngineConstants,
                        matchJuffersWE.get(matchIx),
                        matchParams.getLuceneQueryMatchData(),
                        matchIx,
                        storageCollectorArgs.numChunksInRange(),
                        lucenePageCacheStats,
                        statsDispatcherPageSource,
                        globalConfig);
            }
            matchIx++;
        }
    }

    private void loadDictionaries()
    {
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

    /**
     * prepare buffers for filling
     */
    @NativeInterrupt
    void queryOpen(int rowsLimit)
    {
        bufferAllocator.readerOnAllocBundle();

        if (!dictionariesLoaded) {
            loadDictionaries();
            dictionariesLoaded = true;
        }

        collectOpenResult = collectTxService.collectOpenAndRestore(rowsLimit,
                numRowsCollectedInPrevRounds,
                storageCollectorArgs,
                storeRowListSize,
                storeRowListType);
        collectTxId = collectOpenResult.collectTxId();

        if (queryParams.getNumMatchElements() > 0) {
            try {
                matchTxId = (int) storageEngine.matchOpen(queryParams.getTotalNumRecords(),
                        storageCollectorArgs.collectTxArgs().fileCookie(),
                        collectOpenResult.collectTxId(),
                        storageCollectorArgs.collectTxArgs().collectStoreBuff(),
                        storageCollectorArgs.collectTxArgs().collect2MatchParams(),
                        queryParams.getNumMatchElements(),
                        storageCollectorArgs.numChunksInRange(),
                        weMatchTree,
                        collectOpenResult.matchResultBitmaps().address(),
                        queryParams.getMinMatchOffset(),
                        matchBuffIds);
            }
            catch (Exception e) {
                shapingLogger.warn(e, "matchOpen failed"); // we dont re-throw, we will throw in the next if since matchTxId was not set
            }
            if (matchTxId < 0) {
                collectTxService.freeCollectOpenResources(collectOpenResult);
                throw new TrinoException(WARP_TX_ALLOCATION_FAILED, "failed to allocate tx for match");
            }
            matchedChunksIndexes = new short[storageCollectorArgs.numChunksInRange()];
            matchBitmapResetPoints = new int[storageCollectorArgs.numChunksInRange()];
        }

        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
            // only for lucene
            matchJuffersWE.get(matchIx).createBuffers(
                    matchParams.getRecTypeCode(),
                    matchParams.getRecTypeLength(),
                    false,
                    matchBuffIds[matchIx]);
            matchIx++;
        }
        numRowsCollectedInCurRound = 0;
    }

    @SuppressWarnings("Finally")
    @NativeInterrupt
    private void matchIfNeeded()
    {
        if (matchExhausted) {
            ChunksQueue chunksQueue = storageCollectorArgs.chunksQueue();
            if (matchTxId == INVALID_TX_ID) {
                matchExhausted = chunksQueueService.updateChunkRangeFullScan(chunksQueue, storageCollectorArgs.numChunks(), storageCollectorArgs.numChunksInRange());
                logger.debug("matchIfNeeded matchExhausted %b after full scan update numChunks %d range %d", matchExhausted, storageCollectorArgs.numChunks(), storageCollectorArgs.numChunksInRange());
            }
            else {
                long matchResult = 0;
                long numChunks = 0;
                boolean luceneSuccess = true;
                int numMatchedChunks = 0;
                int chunkIndex = chunksQueueService.getChunkIndexForMatch(chunksQueue);
                // we loop until either agg result returnes 0 which  means no more chunks (break under if inside the loop)
                // or if numMatchedChunks returned positive from match call which means at least one chunk has a match
                // in addition, on every call to storage engine we check for error
                try {
                    while (numMatchedChunks == 0) { // no match so far
                        numChunks = storageEngine.matchAgg(matchTxId, chunkIndex);
                        if (numChunks < 0) {
                            break;
                        }
                        matchExhausted = numChunks == 0;
                        if (matchExhausted) {
                            break;
                        }

                        if (queryParams.getNumLucene() > 0) {
                            for (int luceneMatcherIx = 0; luceneMatcherIx < luceneMatchers.length; luceneMatcherIx++) {
                                if (!luceneMatchers[luceneMatcherIx].match(matchTxId, chunkIndex, (int) numChunks)) {
                                    luceneSuccess = false;
                                    break;
                                }
                            }
                            if (!luceneSuccess) {
                                break;
                            }
                        }

                        matchResult = storageEngine.match(matchTxId, chunkIndex, (int) numChunks, matchedChunksIndexes, matchBitmapResetPoints);
                        if (matchResult < 0) {
                            break;
                        }
                        chunkIndex = (int) (matchResult & MATCH_RESULT_MASK);
                        numMatchedChunks = (int) (matchResult >> 32);
                        logger.debug("matchResult %x chunkIndex %d numMatchedChunks %d", matchResult, chunkIndex, numMatchedChunks);
                    }
                }
                catch (Exception e) {
                    abortMatch(Optional.of(e)); // will close only the match tx here. the caller will close the collect tx
                    // We can't throw the original exception cause it will skip closing the collect TX.
                    // But we do need to preserve the recoverable notion from native.
                    if (e instanceof TrinoException trinoException && trinoException.getErrorCode().equals(WARP_NATIVE_UNRECOVERABLE_ERROR.toErrorCode())) {
                        throw new TrinoException(WARP_UNRECOVERABLE_MATCH_FAILED, "failed to match: " + e.getMessage());
                    }
                    else {
                        throw new TrinoException(WARP_MATCH_FAILED, "failed to match: " + e.getMessage());
                    }
                }
                finally {
                    if ((numChunks < 0) || !luceneSuccess || (matchResult < 0)) {
                        abortMatch(Optional.empty()); // will close only the match tx here. the caller will close the collect tx
                        throw new TrinoException(WARP_UNRECOVERABLE_MATCH_FAILED,
                                "match failed chunkIndex " + chunkIndex + " numChunks " + numChunks + " lucene " + luceneSuccess + " match " + matchResult);
                    }
                }

                if (!matchExhausted) {
                    logger.debug("matchIfNeeded matchStartChunkIndex %d matchEndChunkIndex %d numMatchedChunks %d",
                            chunksQueueService.getChunkIndexForMatch(chunksQueue), chunkIndex, numMatchedChunks);
                    chunksQueueService.updateChunkRangeAfterMatch(chunksQueue, chunkIndex, numMatchedChunks, matchedChunksIndexes, matchBitmapResetPoints);
                }
            }
        }
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean matchAndCollect(boolean isMatchGetNumRanges)
    {
        long startTime = readTimeMeasurement.getStartTime();

        if (collectOpenResult.collectTxId() == INVALID_TX_ID) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }
        CollectBufferState collectBufferState = CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
        matchIfNeeded();
        // we continue as long as buffer is not full and we have more chunks to match and collect
        while (!matchExhausted && (collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_FULL)) {
            CollectFromStorageResult collectFromStorageResult = storageCollectorService.collectFromStorage(collectOpenResult,
                    storageCollectorArgs,
                    isMatchGetNumRanges,
                    numRowsCollectedInCurRound,
                    queryResultType);
            collectBufferState = collectFromStorageResult.collectBufferState();
            numRowsCollectedInCurRound = collectFromStorageResult.numCollectedRows();
            matchExhausted = chunksQueueService.isChunkRangeCompleted(storageCollectorArgs.chunksQueue());
            matchIfNeeded();
        }

        readTimeMeasurement.updateRuntimeMeasurements(startTime, storageCollectorArgs);
        return collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
    }

    private int fillBlocks(Block[] blocks)
    {
        int rowsToFill = Math.min(numRowsCollectedInCurRound, collectOpenResult.rowsLimit());

        storageCollectorService.fillBlocks(blocks,
                storageCollectorArgs,
                rowsToFill,
                numRowsCollectedInPrevRounds,
                queryResultType,
                statsDispatcherPageSource);
        statsDispatcherPageSource.addcached_read_rows(rowsToFill);
        return rowsToFill;
    }

    ReadResult getPage(Block[] blocks, boolean isMatchGetNumRanges)
    {
        int numCollectedRows = 0;
        WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;
        if (matchAndCollect(isMatchGetNumRanges)) {
            numCollectedRows = fillBlocks(blocks);
            if (isMatchGetNumRanges) {
                ranges = storageCollectorService.collectRanges(collectOpenResult);
            }
        }
        return new ReadResult(numCollectedRows, ranges);
    }

    @NativeInterrupt
    long queryClose()
    {
        // match
        if (matchTxId != INVALID_TX_ID) {
            storageEngine.matchClose(matchTxId);
            matchTxId = INVALID_TX_ID;
        }

        if (collectTxId == INVALID_TX_ID) {
            return 0;
        }

        numRowsCollectedInPrevRounds += numRowsCollectedInCurRound;
        CollectCloseResult collectCloseResult = collectTxService.collectStoreAndClose(collectOpenResult,
                storageCollectorArgs,
                numRowsCollectedInCurRound,
                storeRowListSize,
                storeRowListType);
        storeRowListSize = collectCloseResult.storeRowListResult().storeRowListSize();
        storeRowListType = collectCloseResult.storeRowListResult().storeRowListType();
        collectTxId = INVALID_TX_ID;

        bufferAllocator.readerOnFreeBundle();

        return collectCloseResult.readPages();
    }

    private void abortMatch(Optional<Exception> e)
    {
        if (matchTxId != INVALID_TX_ID) {
            boolean nativeThrowed = false;
            if (e.isPresent() && (e.get() instanceof TrinoException)) {
                nativeThrowed = ExceptionThrower.isNativeException((TrinoException) e.get());
            }
            if (!nativeThrowed) {
                storageEngine.matchClose(matchTxId);
            }
            matchTxId = INVALID_TX_ID;
        }
    }

    private void abortCollect(Exception e)
    {
        collectTxService.collectAbort(e, collectOpenResult, collectTxId);
        collectTxId = INVALID_TX_ID;
    }

    void queryAbort(Exception e)
    {
        abortMatch(Optional.of(e));
        abortCollect(e);
        bufferAllocator.readerOnFreeBundle();
    }
}
