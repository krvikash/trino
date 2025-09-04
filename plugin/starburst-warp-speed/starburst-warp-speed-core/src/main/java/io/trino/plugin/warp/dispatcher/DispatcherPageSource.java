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
package io.trino.plugin.warp.dispatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.read.PrefilledPageSource;
import io.trino.plugin.warp.storage.read.WarpStoragePageSource;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.StringJoiner;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.warp.WarpErrorCode.WARP_FAILED_TO_ADD_COLUMN_TO_BUILDER;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DispatcherPageSource
        implements ConnectorPageSource
{
    private static final int MINIMUM_OUTPUT_ROW_COUNT = 256;
    private static final int START_INDEX_OF_PROXIED_CONNECTOR_COLUMNS = 0;
    private static final Logger logger = Logger.get(DispatcherPageSource.class);
    private static final int pageSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    private final ShapingLogger shapingLogger;
    protected final WarpStoragePageSource warpPageSource;
    private final DispatcherSplit dispatcherSplit;
    private final DispatcherTableHandle dispatcherTableHandle;
    private final long maxEmptyPageSourceIterations;
    private final PrefilledPageSource prefilledPageSource;
    private final QueryContext queryContext;
    private final RowGroupData rowGroupData;
    private final PageSourceDecision pageSourceDecision;
    private final DispatcherPageSourceStats stats;
    private final QueryClassifier queryClassifier;
    private final Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider;
    private final ReadErrorHandler readErrorHandler;
    private final RowGroupCloseHandler closeHandler;
    private final long startTime;
    private final List<Type> warpWithoutPrefilledAndProxiedCollectTypes;
    private ConnectorPageSource proxiedConnectorPageSource;
    private SourcePage currentProxiedPage;
    private int currentProxiedPagePosition; // Position upto which currentProxiedPage has been consumed
    private final Deque<RowRange> proxiedPageRanges;
    private long proxiedPagePositionsRead;
    private boolean wasProxiedPagedLoaded;
    private SourcePage currentWarpSourcePage;
    private int currentWarpPagePosition; // Position upto which currentWarpPage has been consumed
    private Deque<RowRange> warpPageRanges;
    private int emptyPagesCounter;
    private boolean forceFinish;

    public DispatcherPageSource(Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider,
            QueryClassifier queryClassifier,
            List<Type> warpWithoutPrefilledAndProxiedCollectTypes,
            WarpStoragePageSource warpPageSource,
            QueryContext queryContext,
            RowGroupData rowGroupData,
            PageSourceDecision pageSourceDecision,
            DispatcherPageSourceStats stats,
            RowGroupCloseHandler closeHandler,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            long deletedRowsCount,
            ReadErrorHandler readErrorHandler,
            GlobalConfig globalConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.proxiedConnectorPageSourceProvider = proxiedConnectorPageSourceProvider;
        this.queryClassifier = queryClassifier;
        this.warpPageSource = warpPageSource;
        this.dispatcherSplit = dispatcherSplit;
        this.dispatcherTableHandle = dispatcherTableHandle;
        this.maxEmptyPageSourceIterations = Math.max(deletedRowsCount, globalConfig.getEmptyPageIterations());
        this.prefilledPageSource = new PrefilledPageSource(
                queryContext.getPrefilledQueryCollectDataByBlockIndex(),
                stats,
                rowGroupData,
                queryContext.getTotalRecords(),
                Optional.of(closeHandler));
        this.queryContext = queryContext;
        this.rowGroupData = rowGroupData;
        this.pageSourceDecision = pageSourceDecision;
        this.stats = stats;
        this.closeHandler = requireNonNull(closeHandler);
        this.readErrorHandler = requireNonNull(readErrorHandler);
        this.proxiedPageRanges = new ArrayDeque<>();
        this.warpPageRanges = new ArrayDeque<>();
        this.startTime = System.nanoTime();
        this.shapingLogger = shapingLoggerFactory.getInstance(DispatcherPageSource.class);
        this.warpWithoutPrefilledAndProxiedCollectTypes = warpWithoutPrefilledAndProxiedCollectTypes;
        this.emptyPagesCounter = 0;
        this.forceFinish = false;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        boolean finishedOnPractice = warpPageRanges.isEmpty() && warpPageSource.isFinished();

        if (emptyPagesCounter == maxEmptyPageSourceIterations) {
            String info = String.format(Locale.US, "queryId=%s, finishedOnPractice=%b, pageSourceDecision=%s, currentProxiedPagePosition=%s, proxiedConnectorPageSource.isFinished=%s, proxiedPageRanges=%s, warpPageRanges.size=%s, currentWarpPagePosition=%s, " +
                            "currentProxiedPage.getPositionCount()=%s, currentWarpPage.getPositionCount()=%s, warpWithoutPrefilledAndProxiedCollectTypes=%s, proxiedPagePositionsRead=%s, proxiedConnectorPageSource=%s, " +
                            "wasProxiedPagedLoaded=%s, dispatcherTableHandle=%s, dispatcherSplit=%s, rowGroupKey=%s",
                    queryContext.getQueryId(),
                    finishedOnPractice,
                    pageSourceDecision,
                    currentProxiedPagePosition,
                    proxiedConnectorPageSource.isFinished(),
                    proxiedPageRanges,
                    warpPageRanges.size(),
                    currentWarpPagePosition,
                    currentProxiedPage.getPositionCount(),
                    currentWarpSourcePage.getPositionCount(),
                    warpWithoutPrefilledAndProxiedCollectTypes,
                    proxiedPagePositionsRead,
                    proxiedConnectorPageSource,
                    wasProxiedPagedLoaded,
                    dispatcherTableHandle,
                    dispatcherSplit,
                    rowGroupData.getRowGroupKey());
            shapingLogger.info("returned more than emptyPagesCounter=%s emptyPages, set forced finished. info=%s", emptyPagesCounter, info);
            forceFinish = true;
        }

        if (finishedOnPractice) {
            // TODO: We return an empty page here because isFinished() might return false even when the page source is actually finished
            //  (we count on Trino to close the page source after reaching the required limit).
            //  This is due to a bug in SubqueryCache. After fixing the bug- please revert the commit that introduced this change
            //  (see https://github.com/trinodb/trino/pull/22827#discussion_r1716813795).
            emptyPagesCounter++;
            return SourcePage.create(0);
        }

        SourcePage dispatcherSourcePage = getDispatcherSourcePage();
        if (dispatcherSourcePage.getPositionCount() == 0 && !isFinished()) {
            emptyPagesCounter++;
        }
        else {
            emptyPagesCounter = 0;
        }
        return dispatcherSourcePage;
    }

    private SourcePage getDispatcherSourcePage()
    {
        try {
            if (PageSourceDecision.WARP.equals(pageSourceDecision)) {
                SourcePage warpSourcePage = warpPageSource.getNextSourcePage();
                return mergeWarpPrefilled(warpSourcePage, prefilledPageSource);
            }

            PageBuilder resultPageBuilder = PageBuilder.withMaxPageSize(pageSizeInBytes, warpWithoutPrefilledAndProxiedCollectTypes);
            if (warpPageRanges.isEmpty()) {
                // Calling getNextPage at least once is necessary to make warp page source populate row ranges
                getNextWarpSourcePage();
                if (warpPageRanges.isEmpty()) {
                    return buildResultPage(resultPageBuilder);
                }
            }

            // Calling getNextPage if it's the first one to make proxied page source populate row ranges
            // Or in case the current page is empty to avoid getting into infinite loop of empty pages
            if (currentProxiedPage == null || currentProxiedPage.getPositionCount() == 0) {
                getNextProxiedPage();
            }

            while (!resultPageBuilder.isFull() && !warpPageRanges.isEmpty() && !proxiedPageRanges.isEmpty()) {
                RowRange warpCurrentRange = warpPageRanges.peek();
                RowRange proxiedCurrentRange = proxiedPageRanges.peek();
                logger.debug(
                        "Going to merge results for a mixed query with predicate. warpCurrentRange=%s, proxiedCurrentRange=%s, currentWarpPagePosition=%d, currentProxiedPagePositions=%d, currentWarpPagePositions=%d, currentProxiedPagePositions=%d",
                        warpCurrentRange, proxiedCurrentRange, currentWarpPagePosition, currentProxiedPagePosition, currentWarpSourcePage.getPositionCount(), currentProxiedPage.getPositionCount());

                if (warpCurrentRange.isFullyBefore(proxiedCurrentRange)) {
                    warpPageRanges.removeFirst();
                    seekWarpPageSource(toIntExact(warpCurrentRange.getRowCount()));
                }
                else if (proxiedCurrentRange.isFullyBefore(warpCurrentRange)) {
                    proxiedPageRanges.removeFirst();
                    seekProxiedPageSource(proxiedCurrentRange.getRowCount());
                }
                else { // There is some overlap in warpCurrentRange and proxiedCurrentRange
                    warpPageRanges.removeFirst();
                    proxiedPageRanges.removeFirst();
                    long overlapStartInclusive = Math.max(warpCurrentRange.minInclusive(), proxiedCurrentRange.minInclusive());
                    long overlapEndExclusive = Math.min(warpCurrentRange.maxExclusive(), proxiedCurrentRange.maxExclusive());
                    // Seek to overlap start in both warp and proxied page
                    currentWarpPagePosition += toIntExact(Math.max(overlapStartInclusive - warpCurrentRange.minInclusive(), 0));
                    seekProxiedPageSource(Math.max(overlapStartInclusive - proxiedCurrentRange.minInclusive(), 0));
                    int overlapRowCount = toIntExact(overlapEndExclusive - overlapStartInclusive);
                    // Collect overlappingRows from warp and proxied pages
                    if (canMergeFull(resultPageBuilder, overlapRowCount)) {
                        SourcePage resultPage = buildFullResultPage(overlapRowCount);
                        long resultEndExclusive = overlapStartInclusive + resultPage.getPositionCount();
                        // Add back any remaining part of row range onto the deque for the next iteration
                        if (warpCurrentRange.maxExclusive() > resultEndExclusive) {
                            warpPageRanges.addFirst(new RowRange(resultEndExclusive, warpCurrentRange.maxExclusive()));
                        }
                        if (proxiedCurrentRange.maxExclusive() > resultEndExclusive) {
                            proxiedPageRanges.addFirst(new RowRange(resultEndExclusive, proxiedCurrentRange.maxExclusive()));
                        }
                        return resultPage;
                    }
                    fillOverlappingPage(resultPageBuilder, overlapRowCount);
                    // Add back any remaining part of row range onto the deque for the next iteration
                    if (warpCurrentRange.maxExclusive() > overlapEndExclusive) {
                        warpPageRanges.addFirst(new RowRange(overlapEndExclusive, warpCurrentRange.maxExclusive()));
                    }
                    else if (proxiedCurrentRange.maxExclusive() > overlapEndExclusive) {
                        proxiedPageRanges.addFirst(new RowRange(overlapEndExclusive, proxiedCurrentRange.maxExclusive()));
                    }
                }
            }
            SourcePage resultPage = buildResultPage(resultPageBuilder);
            if (forceFinish) {
                logger.info("queryId=%s, resultPage positionCount=%s", queryContext.getQueryId(), resultPage.getPositionCount());
            }
            return resultPage;
        }
        catch (Throwable e) {
            stats.inccached_warp_failed_pages();
            if (!Thread.currentThread().isInterrupted()) {
                shapingLogger.error(e,
                        "Failed to read cache file %s from warp. queryContext=%s, rowGroupData=%s, warpWithoutPrefilledAndProxiedCollectTypes=%s, dispatcherTableHandle=%s",
                        rowGroupData.getRowGroupKey(), queryContext, rowGroupData, warpWithoutPrefilledAndProxiedCollectTypes, dispatcherTableHandle);
            }
            readErrorHandler.handle(e, rowGroupData, queryContext);
            throw e;
        }
    }

    private SourcePage mergeWarpPrefilled(SourcePage currentWarpSourcePage,
            PrefilledPageSource prefilledPageSource)
    {
        Block[] orderedBlocks = new Block[queryContext.getTotalCollectCount()];
        int startPointWarp = 0;

        if (currentWarpSourcePage.getPositionCount() == 0 || queryContext.getTotalCollectCount() == 0) {
            return SourcePage.create(currentWarpSourcePage.getPositionCount());
        }

        HashMap<Integer, Integer> warpColumnIndexMap = new HashMap<>();
        for (int i = 0; i < queryContext.getTotalCollectCount(); i++) {
            if (prefilledPageSource.hasBlock(i)) {
                orderedBlocks[i] = prefilledPageSource.createBlock(i, currentWarpSourcePage.getPositionCount());
            }
            else {
                final int warpBlockIndex = queryContext.getNativeQueryCollectDataList()
                        .get(startPointWarp)
                        .getBlockIndex();
                warpColumnIndexMap.put(warpBlockIndex, startPointWarp);
                startPointWarp++;
            }
        }
        return new DispatcherSourcePage(orderedBlocks, currentWarpSourcePage, warpColumnIndexMap);
    }

    private void seekWarpPageSource(int rowsToSkip)
    {
        currentWarpPagePosition += rowsToSkip;
        checkState(
                currentWarpPagePosition <= currentWarpSourcePage.getPositionCount(),
                "currentWarpPagePosition %s, currentWarpPage positions %s, warpPageRanges %s",
                currentWarpPagePosition, currentWarpSourcePage.getPositionCount(), warpPageRanges);
        if (currentWarpPagePosition == currentWarpSourcePage.getPositionCount()) {
            checkState(
                    warpPageRanges.isEmpty(),
                    "currentWarpPagePosition %s, currentWarpPage positions %s, warpPageRanges %s",
                    currentWarpPagePosition, currentWarpSourcePage.getPositionCount(), warpPageRanges);
            getNextWarpSourcePage();
        }
    }

    private void seekProxiedPageSource(long rowsToSkip)
    {
        int pagePositionsLeft = currentProxiedPage.getPositionCount() - currentProxiedPagePosition;
        while (rowsToSkip >= pagePositionsLeft && rowsToSkip > 0) {
            rowsToSkip -= pagePositionsLeft;
            getNextProxiedPage();
            pagePositionsLeft = currentProxiedPage.getPositionCount();
        }
        currentProxiedPagePosition += toIntExact(rowsToSkip);
    }

    /**
     * Proxied page source produces LazyBlocks, but using PageBuilder forces loading of lazy blocks.
     * Therefore, we attempt to use currentProxiedPage directly when it does not lead to too small output page
     */
    private boolean canMergeFull(PageBuilder resultPageBuilder, int overlapRowCount)
    {
        if (!resultPageBuilder.isEmpty()) {
            return false; // We're already building result using PageBuilder
        }
        int pagePositionsLeft = currentProxiedPage.getPositionCount() - currentProxiedPagePosition;
        if (currentProxiedPagePosition == 0 && overlapRowCount >= pagePositionsLeft) {
            return true; // Full proxied page is selected
        }
        return Math.min(pagePositionsLeft, overlapRowCount) >= MINIMUM_OUTPUT_ROW_COUNT;
    }

    private SourcePage buildFullResultPage(int overlapRowCount)
    {
        if (queryContext.getTotalCollectCount() != (currentProxiedPage.getChannelCount() + currentWarpSourcePage.getChannelCount() + prefilledPageSource.getChannelCount())) {
            throw new TrinoException(WarpErrorCode.WARP_FAILED_TO_BUILD_MIXED_PAGE, "wrong number of columns");
        }
        Block[] orderedBlocks = new Block[queryContext.getTotalCollectCount()];
        int startPointWarp = 0;
        int startPointProxied = START_INDEX_OF_PROXIED_CONNECTOR_COLUMNS;
        int positionCount = Math.min(currentProxiedPage.getPositionCount() - currentProxiedPagePosition, overlapRowCount);
        Page overlapPoxiedPage = currentProxiedPage.getPage().getRegion(currentProxiedPagePosition, positionCount);
        recordProxiedPageLoad();
        Page overlapWarpPage = currentWarpSourcePage.getPage().getRegion(currentWarpPagePosition, positionCount);
        for (int i = 0; i < queryContext.getTotalCollectCount(); i++) {
            if (queryContext.getRemainingCollectColumnByBlockIndex().containsKey(i)) {
                orderedBlocks[i] = overlapPoxiedPage.getBlock(startPointProxied);
                startPointProxied++;
            }
            else {
                if (prefilledPageSource.hasBlock(i)) {
                    orderedBlocks[i] = prefilledPageSource.createBlock(i, positionCount);
                }
                else {
                    final int warpBlockIndex = queryContext.getNativeQueryCollectDataList().get(startPointWarp).getBlockIndex();
                    orderedBlocks[warpBlockIndex] = overlapWarpPage.getBlock(startPointWarp);
                    startPointWarp++;
                }
            }
        }

        seekWarpPageSource(positionCount);
        seekProxiedPageSource(positionCount);
        return SourcePage.create(new Page(positionCount, orderedBlocks));
    }

    private void fillOverlappingPage(PageBuilder resultPageBuilder, int numberOfRowsToAdd)
    {
        resultPageBuilder.declarePositions(numberOfRowsToAdd);
        long start = System.nanoTime();
        // Add overlapping rows from proxied page source
        int pagePositionsLeft = currentProxiedPage.getPositionCount() - currentProxiedPagePosition;
        int overlapRowsRemaining = numberOfRowsToAdd;
        while (overlapRowsRemaining >= pagePositionsLeft && overlapRowsRemaining > 0) {
            addColumnsToBuilder(resultPageBuilder, pagePositionsLeft, currentProxiedPage, currentProxiedPagePosition, 0);
            recordProxiedPageLoad();
            overlapRowsRemaining -= pagePositionsLeft;
            getNextProxiedPage();
            pagePositionsLeft = currentProxiedPage.getPositionCount();
            if (proxiedConnectorPageSource.isFinished()) {
                // Cannot reach end of proxied page source before overlap rows are produced
                checkState(
                        overlapRowsRemaining <= pagePositionsLeft,
                        "Reached end of proxied page source with overlapRowsRemaining %s, pagePositionsLeft %s",
                        overlapRowsRemaining,
                        pagePositionsLeft);
            }
        }
        if (overlapRowsRemaining > 0) {
            addColumnsToBuilder(resultPageBuilder, overlapRowsRemaining, currentProxiedPage, currentProxiedPagePosition, 0);
            recordProxiedPageLoad();
            currentProxiedPagePosition += overlapRowsRemaining;
        }
        stats.addproxied_loaded_pages_time(System.nanoTime() - start);

        // Add overlapping rows from currentWarpPage
        addColumnsToBuilder(resultPageBuilder, numberOfRowsToAdd, currentWarpSourcePage, currentWarpPagePosition, queryContext.getRemainingCollectColumnByBlockIndex().size());
        seekWarpPageSource(numberOfRowsToAdd);
    }

    private void recordProxiedPageLoad()
    {
        if (!wasProxiedPagedLoaded) {
            wasProxiedPagedLoaded = true;
            stats.incproxied_loaded_pages();
            stats.addproxied_loaded_pages_bytes(currentProxiedPage.getSizeInBytes());
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return warpPageSource.getCompletedBytes() + (proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getCompletedBytes());
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        if (proxiedConnectorPageSource != null) {
            return proxiedConnectorPageSource.getCompletedPositions();
        }
        return OptionalLong.of(warpPageSource.getCompletedPositions());
    }

    @Override
    public long getReadTimeNanos()
    {
        return proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return forceFinish ||
                (warpPageRanges.isEmpty() &&
                        warpPageSource.isFinished() &&
                        !warpPageSource.isRowsLimitReached());  // TODO: this line was added due to a bug in SubqueryCache. After fixing the bug- please revert the commit that introduced this change (see https://github.com/trinodb/trino/pull/22827#discussion_r1716813795).
    }

    @Override
    public long getMemoryUsage()
    {
        return warpPageSource.getMemoryUsage() + (proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getMemoryUsage());
    }

    @Override
    public void close()
            throws IOException
    {
        //don't call prefilledPageSource.close since it is not used as a ConnectorPageSource here
        try {
            boolean success = true;
            StringJoiner errorMsg = new StringJoiner(",");
            try {
                warpPageRanges.clear();
                currentWarpSourcePage = null;
                warpPageSource.close();
            }
            catch (Exception e) {
                errorMsg.add(e.getMessage());
                success = false;
            }
            try {
                if (proxiedConnectorPageSource != null) {
                    proxiedPageRanges.clear();
                    currentProxiedPage = null;
                    proxiedConnectorPageSource.close();
                }
            }
            catch (Exception e) {
                errorMsg.add(e.getMessage());
                success = false;
            }
            if (success) {
                stats.inccached_warp_success_files();
            }
            else {
                stats.inccached_warp_failed_files();
                errorMsg.add(format("failed to close file %s", rowGroupData.getRowGroupKey()));
                throw new IOException(errorMsg.toString());
            }
        }
        finally {
            queryClassifier.close(queryContext);
            this.stats.addexecution_time(System.nanoTime() - this.startTime);
            closeHandler.accept(rowGroupData, "close page source", shapingLogger);
        }
    }

    @Override
    public Metrics getMetrics()
    {
        return proxiedConnectorPageSource == null ? Metrics.EMPTY : proxiedConnectorPageSource.getMetrics();
    }

    private void getNextWarpSourcePage()
    {
        currentWarpSourcePage = requireNonNull(warpPageSource.getNextSourcePage(), "warpPageSource returned a null Page");
        currentWarpPagePosition = 0;
        // WarpPageSource#getSortedRowRanges always returns row ranges for the Page returned from previous call of WarpPageSource#getNextPage
        // It may return a smaller Page than the row ranges when LIMIT is reached
        WarpStoragePageSource.RowRanges warpRowRanges = warpPageSource.getSortedRowRanges();
        if (warpPageSource.isRowsLimitReached()) {
            validateRanges(
                    warpRowRanges.getRowCount() >= currentWarpSourcePage.getPositionCount(),
                    "Row ranges %s are smaller than page positions count %s",
                    warpRowRanges,
                    currentWarpSourcePage.getPositionCount());
        }
        else {
            validateRanges(
                    warpRowRanges.getRowCount() == currentWarpSourcePage.getPositionCount(),
                    "Mismatch in row ranges %s and page positions count %s",
                    warpRowRanges,
                    currentWarpSourcePage.getPositionCount());
        }
        Deque<RowRange> sortedRowRangesWithLimit = new ArrayDeque<>();
        int rowRangeToCollect = currentWarpSourcePage.getPositionCount();
        for (int rangeIndex = 0; rangeIndex < warpRowRanges.getRangesCount() && rowRangeToCollect > 0; rangeIndex++) {
            long lowerInclusive = warpRowRanges.getLowerInclusive(rangeIndex);
            long upperExclusive = warpRowRanges.getUpperExclusive(rangeIndex);
            int rangeRowCount = toIntExact(upperExclusive - lowerInclusive);
            if (rangeRowCount <= rowRangeToCollect) {
                sortedRowRangesWithLimit.addLast(new RowRange(lowerInclusive, upperExclusive));
                rowRangeToCollect -= rangeRowCount;
            }
            else {
                sortedRowRangesWithLimit.addLast(new RowRange(lowerInclusive, lowerInclusive + rowRangeToCollect));
                rowRangeToCollect = 0;
            }
        }
        warpPageRanges = sortedRowRangesWithLimit;
    }

    private void getNextProxiedPage()
    {
        long start = System.nanoTime();
        if (proxiedConnectorPageSource == null) {
            if (forceFinish) {
                logger.info("queryId=%s, creating new page source - SHOULD NOT HAPPEN", queryContext.getQueryId());
            }
            proxiedConnectorPageSource = proxiedConnectorPageSourceProvider.get();
        }
        currentProxiedPage = null;
        wasProxiedPagedLoaded = false;
        while (currentProxiedPage == null && !proxiedConnectorPageSource.isFinished()) {
            currentProxiedPage = proxiedConnectorPageSource.getNextSourcePage();
            if (currentProxiedPage != null && currentProxiedPage.getPositionCount() > 0) {
                proxiedPageRanges.add(new RowRange(proxiedPagePositionsRead, proxiedPagePositionsRead + currentProxiedPage.getPositionCount()));
            }
        }
        currentProxiedPagePosition = 0;
        if (currentProxiedPage == null) {
            if (forceFinish) {
                logger.info("queryId=%s, currentProxiedPage is null and pageSource isFinished? %s", queryContext.getQueryId(), proxiedConnectorPageSource.isFinished());
            }
            // proxiedConnectorPageSource is finished, return an empty page
            currentProxiedPage = SourcePage.create(0);
        }
        else {
            if (forceFinish) {
                logger.info("queryId=%s, currentProxiedPage is not null and position is %s, proxiedPageRanges=%s", queryContext.getQueryId(), currentProxiedPage.getPositionCount(), proxiedPageRanges);
            }
            proxiedPagePositionsRead += currentProxiedPage.getPositionCount();
            stats.incproxied_pages();
        }
        stats.addproxied_time(System.nanoTime() - start);
    }

    private SourcePage buildResultPage(PageBuilder resultPageBuilder)
    {
        Page resultPage = resultPageBuilder.build();
        int blocksCount = queryContext.getTotalCollectCount();

        if (blocksCount != resultPage.getChannelCount() + prefilledPageSource.getChannelCount()) {
            throw new TrinoException(WarpErrorCode.WARP_FAILED_TO_BUILD_MIXED_PAGE, "wrong number of columns");
        }
        Block[] orderedBlocks = new Block[blocksCount];
        final int warpStartIndex = queryContext.getRemainingCollectColumnByBlockIndex().size();
        int warpColumnIndex = warpStartIndex;
        int proxiedConnectorColumnIndex = START_INDEX_OF_PROXIED_CONNECTOR_COLUMNS;
        for (int i = 0; i < queryContext.getTotalCollectCount(); i++) {
            if (queryContext.getRemainingCollectColumnByBlockIndex().containsKey(i)) {
                orderedBlocks[i] = resultPage.getBlock(proxiedConnectorColumnIndex);
                proxiedConnectorColumnIndex++;
            }
            else {
                if (prefilledPageSource.hasBlock(i)) {
                    orderedBlocks[i] = prefilledPageSource.createBlock(i, resultPage.getPositionCount());
                }
                else {
                    final int warpBlockIndex = queryContext.getNativeQueryCollectDataList().get(warpColumnIndex - warpStartIndex).getBlockIndex();
                    orderedBlocks[warpBlockIndex] = resultPage.getBlock(warpColumnIndex);
                    warpColumnIndex++;
                }
            }
        }
        resultPageBuilder.reset();
        return SourcePage.create(new Page(orderedBlocks));
    }

    private static void appendBlockRange(Block block, int offset, int length, BlockBuilder blockBuilder)
    {
        switch (block) {
            case RunLengthEncodedBlock rleBlock -> blockBuilder.appendRepeated(rleBlock.getValue(), 0, length);
            case DictionaryBlock dictionaryBlock ->
                    blockBuilder.appendPositions(dictionaryBlock.getDictionary(), dictionaryBlock.getRawIds(), dictionaryBlock.getRawIdsOffset() + offset, length);
            case ValueBlock valueBlock -> blockBuilder.appendRange(valueBlock, offset, length);
        }
    }

    private void addColumnsToBuilder(PageBuilder resultPageBuilder,
            int numberOfRowsToAdd,
            SourcePage sourcePage,
            int currentRowInPage,
            int columnInBuilder)
    {
        for (int column = 0; column < sourcePage.getChannelCount(); column++) {
            BlockBuilder blockBuilder = resultPageBuilder.getBlockBuilder(columnInBuilder);
            try {
                appendBlockRange(sourcePage.getBlock(column), currentRowInPage, numberOfRowsToAdd, blockBuilder);
            }
            catch (Exception e) {
                throw new TrinoException(
                        WARP_FAILED_TO_ADD_COLUMN_TO_BUILDER,
                        format("Failed to add column %d to builder. numberOfRowsToAdd=%d, currentRowInPage=%d, columnInBuilder=%d",
                                column, numberOfRowsToAdd, currentRowInPage, columnInBuilder),
                        e);
            }
            columnInBuilder++;
        }
    }

    @VisibleForTesting
    PageSourceDecision getPageSourceDecision()
    {
        return pageSourceDecision;
    }

    private record RowRange(long minInclusive, long maxExclusive)
    {
        public RowRange
        {
            checkArgument(
                    minInclusive < maxExclusive && minInclusive >= 0,
                    "minInclusive %s must be smaller than maxExclusive %s",
                    minInclusive,
                    maxExclusive);
        }

        public boolean isFullyBefore(RowRange other)
        {
            return maxExclusive <= other.minInclusive();
        }

        public long getRowCount()
        {
            return maxExclusive - minInclusive;
        }
    }

    // TODO add @FormatMethod after implementing ConnectorPageSource.RowRanges#toString
    @SuppressWarnings("AnnotateFormatMethod")
    //@FormatMethod
    private static void validateRanges(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new TrinoException(WarpErrorCode.WARP_MATCH_RANGES_ERROR, format(formatString, args));
        }
    }

    private static class DispatcherSourcePage
            implements SourcePage
    {
        private final Block[] blocks;
        private final SourcePage warpSourcePage;
        private final Map<Integer, Integer> warpIxMap;

        public DispatcherSourcePage(Block[] blocks,
                                    SourcePage warpSourcePage,
                                    Map<Integer, Integer> warpIxMap)
        {
            this.blocks = blocks;
            this.warpSourcePage = warpSourcePage;
            this.warpIxMap = warpIxMap;
        }

        @Override
        public int getPositionCount()
        {
            return warpSourcePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            long sizeInBytes = 0;
            for (Block block : blocks) {
                if (block != null) {
                    sizeInBytes += block.getSizeInBytes();
                }
            }
            return sizeInBytes;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            long retainedSizeInBytes = 0;
            for (Block block : blocks) {
                if (block != null) {
                    retainedSizeInBytes += block.getRetainedSizeInBytes();
                }
            }
            return retainedSizeInBytes;
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
        }

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            if (blocks[channel] == null) {
                blocks[channel] = warpSourcePage.getBlock(warpIxMap.get(channel));
            }
            return blocks[channel];
        }

        @Override
        public Page getPage()
        {
            for (int channel = 0; channel < blocks.length; channel++) {
                getBlock(channel);
            }
            // TODO get multiple blocks from warpSourcePage at once
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            for (int i = 0; i < blocks.length; i++) {
                if (blocks[i] != null) {
                    blocks[i] = blocks[i].getPositions(positions, offset, size);
                }
            }
            warpSourcePage.selectPositions(positions, offset, size);
        }
    }
}
