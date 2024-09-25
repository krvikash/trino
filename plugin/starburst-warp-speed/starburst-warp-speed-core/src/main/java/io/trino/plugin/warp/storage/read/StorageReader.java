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
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.TestStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import java.util.Optional;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static java.util.Objects.requireNonNull;

public class StorageReader
{
    private static final Logger logger = Logger.get(StorageReader.class);
    private final ShapingLogger shapingLogger;

    // services
    private final DictionaryStats dictionaryStats;
    private final DispatcherPageSourceStats statsDispatcherPageSource;
    private final DictionaryCacheService dictionaryCacheService;
    private final TestStats testStats;

    // parameters
    private final QueryParams queryParams;
    private final ReadTimeMeasurement readTimeMeasurement;

    private final QueryArgs queryArgs;
    private final StorageCollectorArgs storageCollectorArgs;
    private final MatchArgs matchArgs;

    private final StorageCollectorService storageCollectorService;
    private final MatchService matchService;

    private int numRowsCollectedInCurRound; // num rows collected in this getNextPage
    private int numRowsCollectedInPrevRounds; // num rows collected in all previous getNextPages
    private int[] queryResultType;
    private boolean dictionariesLoaded;
    private Optional<StoreRowListResult> storeRowListResult;
    private CollectOpenResult collectOpenResult;
    private MatchOpenResult matchOpenResult;

    StorageReader(DictionaryCacheService dictionaryCacheService,
            QueryParams queryParams,
            CustomStatsContext customStatsContext,
            StorageCollectorService storageCollectorService,
            MatchService matchService,
            GlobalConfig globalConfig)
    {
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.matchService = requireNonNull(matchService);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.statsDispatcherPageSource = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);
        this.dictionaryStats = (DictionaryStats) customStatsContext.getStat(DictionaryCacheService.DICTIONARY_STAT_GROUP);
        this.testStats = (TestStats) customStatsContext.getStat("test");

        this.queryParams = queryParams;
        this.queryArgs = storageCollectorService.getQueryArgs(queryParams);
        this.storageCollectorArgs = storageCollectorService.getStorageCollectorArgs(queryArgs);

        this.queryResultType = new int[queryParams.getNumCollectElements()];
        this.storeRowListResult = Optional.empty();

        storageCollectorService.init(queryArgs);

        this.matchArgs = matchService.init(queryArgs, customStatsContext);
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        readTimeMeasurement = new ReadTimeMeasurement();
    }

    void close()
    {
        storageCollectorService.terminate(queryArgs);
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
        if (!dictionariesLoaded) {
            loadDictionaries();
            dictionariesLoaded = true;
        }
        collectOpenResult = storageCollectorService.open(queryArgs, storageCollectorArgs, numRowsCollectedInPrevRounds, rowsLimit, storeRowListResult);

        try {
            matchOpenResult = matchService.open(queryArgs, matchArgs, collectOpenResult);
        }
        catch (Exception e) {
            queryAbort(e);
            throw new TrinoException(WARP_MATCH_FAILED, "failed to allocate tx for match");
        }

        numRowsCollectedInCurRound = 0;
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean matchAndCollect(boolean isMatchGetNumRanges)
    {
        long startTime = readTimeMeasurement.getStartTime();

        if (collectOpenResult == null) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }
        CollectBufferState collectBufferState = CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;

        // we continue as long as buffer is not full and we have more chunks to match and collect
        while ((collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_FULL) && matchService.match(queryArgs, matchArgs, matchOpenResult)) {
            CollectFromStorageResult collectFromStorageResult = storageCollectorService.collectFromStorage(queryArgs,
                    collectOpenResult,
                    isMatchGetNumRanges,
                    numRowsCollectedInCurRound,
                    queryResultType);
            collectBufferState = collectFromStorageResult.collectBufferState();
            numRowsCollectedInCurRound = collectFromStorageResult.numCollectedRows();
        }

        readTimeMeasurement.updateRuntimeMeasurements(startTime, queryArgs);
        return collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
    }

    private int fillBlocks(Block[] blocks)
    {
        int rowsToFill = Math.min(numRowsCollectedInCurRound, collectOpenResult.rowsLimit());

        storageCollectorService.fillBlocks(blocks,
                queryArgs,
                storageCollectorArgs,
                rowsToFill,
                numRowsCollectedInPrevRounds,
                queryResultType,
                statsDispatcherPageSource,
                testStats);
        statsDispatcherPageSource.addcached_read_rows(rowsToFill);
        return rowsToFill;
    }

    ReadResult getPage(Block[] blocks, boolean isMatchGetNumRanges)
    {
        int numCollectedRows = 0;
        WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;
        try {
            if (matchAndCollect(isMatchGetNumRanges)) {
                numCollectedRows = fillBlocks(blocks);
                if (isMatchGetNumRanges) {
                    ranges = storageCollectorService.collectRanges(collectOpenResult);
                }
            }
        }
        catch (Exception e) {
            queryAbort(e);
            throw e;
        }
        return new ReadResult(numCollectedRows, ranges);
    }

    @NativeInterrupt
    long queryClose()
    {
        // match

        if (matchOpenResult != null) {
            matchService.close(matchOpenResult);
            matchOpenResult = null;
        }

        if (collectOpenResult == null) {
            return 0;
        }

        numRowsCollectedInPrevRounds += numRowsCollectedInCurRound;
        CollectCloseResult collectCloseResult = storageCollectorService.close(queryArgs,
                collectOpenResult,
                storageCollectorArgs,
                numRowsCollectedInCurRound,
                testStats);
        collectOpenResult = null;
        storeRowListResult = collectCloseResult.storeRowListResult();
        return collectCloseResult.readPages();
    }

    private void queryAbort(Exception e)
    {
        if (matchOpenResult != null) {
            matchService.abort(matchOpenResult, e);
            matchOpenResult = null;
        }
        if (collectOpenResult != null) {
            storageCollectorService.abort(collectOpenResult, e);
            collectOpenResult = null;
        }
    }
}
