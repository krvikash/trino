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
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.read.PrefilledPageSource;
import io.trino.plugin.warp.storage.read.WarpStoragePageSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongConsumer;
import java.util.function.ObjLongConsumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DispatcherPageSource
        implements ConnectorPageSource
{
    private final ShapingLogger shapingLogger;
    protected final WarpStoragePageSource warpPageSource;
    private final DispatcherTableHandle dispatcherTableHandle;
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
    private SourcePage lastLoadRecordedProxiedPage;
    private long lastLoadRecordedProxiedPageSizeInBytes;
    private SourcePage currentWarpSourcePage;
    private int currentWarpPagePosition; // Position upto which currentWarpPage has been consumed
    private Deque<RowRange> warpPageRanges;
    // Output layout is fixed per split; each output channel is served by exactly one of proxied, prefilled or warp
    private final int[] proxiedOutputChannels;
    private final int[] prefilledOutputChannels;
    private final int[] warpOutputChannels;
    private final int[] warpIndexByOutputChannel;
    private final int[] proxiedIndexByOutputChannel;

    public DispatcherPageSource(
            Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider,
            QueryClassifier queryClassifier,
            List<Type> warpWithoutPrefilledAndProxiedCollectTypes,
            WarpStoragePageSource warpPageSource,
            QueryContext queryContext,
            RowGroupData rowGroupData,
            PageSourceDecision pageSourceDecision,
            DispatcherPageSourceStats stats,
            RowGroupCloseHandler closeHandler,
            DispatcherTableHandle dispatcherTableHandle,
            ReadErrorHandler readErrorHandler,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.proxiedConnectorPageSourceProvider = proxiedConnectorPageSourceProvider;
        this.queryClassifier = queryClassifier;
        this.warpPageSource = warpPageSource;
        this.dispatcherTableHandle = dispatcherTableHandle;
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
        this.proxiedOutputChannels = queryContext.getRemainingCollectColumnByBlockIndex().keySet().stream()
                .sorted()
                .mapToInt(Integer::intValue)
                .toArray();
        this.prefilledOutputChannels = IntStream.range(0, queryContext.getTotalCollectCount())
                .filter(channel -> !queryContext.getRemainingCollectColumnByBlockIndex().containsKey(channel) && prefilledPageSource.hasBlock(channel))
                .toArray();
        this.warpOutputChannels = queryContext.getNativeQueryCollectDataList().stream()
                .mapToInt(NativeQueryCollectData::getBlockIndex)
                .toArray();
        this.warpIndexByOutputChannel = new int[queryContext.getTotalCollectCount()];
        Arrays.fill(warpIndexByOutputChannel, -1);
        for (int warpIndex = 0; warpIndex < warpOutputChannels.length; warpIndex++) {
            warpIndexByOutputChannel[warpOutputChannels[warpIndex]] = warpIndex;
        }
        this.proxiedIndexByOutputChannel = new int[queryContext.getTotalCollectCount()];
        Arrays.fill(proxiedIndexByOutputChannel, -1);
        for (int proxiedIndex = 0; proxiedIndex < proxiedOutputChannels.length; proxiedIndex++) {
            proxiedIndexByOutputChannel[proxiedOutputChannels[proxiedIndex]] = proxiedIndex;
        }
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        try {
            if (PageSourceDecision.WARP.equals(pageSourceDecision)) {
                SourcePage warpSourcePage = warpPageSource.getNextSourcePage();
                return mergeWarpPrefilled(warpSourcePage);
            }

            if (warpPageRanges.isEmpty()) {
                // Calling getNextPage at least once is necessary to make warp page source populate row ranges
                getNextWarpSourcePage();
                if (warpPageRanges.isEmpty()) {
                    return SourcePage.create(0);
                }
            }

            // Fetches the next proxied page when none is current or the current one is empty
            if (currentProxiedPage == null || currentProxiedPage.getPositionCount() == 0) {
                getNextProxiedPage();
            }

            while (!warpPageRanges.isEmpty() && !proxiedPageRanges.isEmpty()) {
                RowRange warpCurrentRange = warpPageRanges.peek();
                RowRange proxiedCurrentRange = proxiedPageRanges.peek();
                if (warpCurrentRange.isFullyBefore(proxiedCurrentRange)) {
                    warpPageRanges.removeFirst();
                    seekWarpPageSource(toIntExact(warpCurrentRange.getRowCount()));
                }
                else if (proxiedCurrentRange.isFullyBefore(warpCurrentRange)) {
                    proxiedPageRanges.removeFirst();
                    skipCurrentProxiedPage(proxiedCurrentRange.getRowCount());
                }
                else { // There is some overlap in warpCurrentRange and proxiedCurrentRange
                    return mergeCurrentProxiedPage();
                }
            }
            if (!warpPageRanges.isEmpty() && proxiedConnectorPageSource.isFinished()) {
                throw new TrinoException(
                        WarpErrorCode.WARP_PROXIED_ROW_MISMATCH,
                        format("Proxied page source finished after %s rows before warp match ranges %s", proxiedPagePositionsRead, warpPageRanges));
            }
            return SourcePage.create(0);
        }
        catch (Throwable e) {
            stats.inccached_warp_failed_pages();
            if (!Thread.currentThread().isInterrupted()) {
                shapingLogger.error(
                        e,
                        "Failed to read cache file %s from warp. queryContext=%s, rowGroupData=%s, warpWithoutPrefilledAndProxiedCollectTypes=%s, dispatcherTableHandle=%s",
                        rowGroupData.getRowGroupKey(),
                        queryContext,
                        rowGroupData,
                        warpWithoutPrefilledAndProxiedCollectTypes,
                        dispatcherTableHandle);
            }
            readErrorHandler.handle(e, rowGroupData, queryContext);
            throw e;
        }
    }

    private SourcePage mergeWarpPrefilled(SourcePage currentWarpSourcePage)
    {
        if (currentWarpSourcePage.getPositionCount() == 0 || queryContext.getTotalCollectCount() == 0) {
            return SourcePage.create(currentWarpSourcePage.getPositionCount());
        }

        Block[] orderedBlocks = new Block[queryContext.getTotalCollectCount()];
        for (int channel : prefilledOutputChannels) {
            orderedBlocks[channel] = prefilledPageSource.createBlock(channel, currentWarpSourcePage.getPositionCount());
        }
        return new MergedSourcePage(
                orderedBlocks,
                currentWarpSourcePage,
                warpIndexByOutputChannel,
                currentWarpSourcePage.getPositionCount(),
                _ -> {});
    }

    private void seekWarpPageSource(int rowsToSkip)
    {
        currentWarpPagePosition += rowsToSkip;
        checkState(
                currentWarpPagePosition <= currentWarpSourcePage.getPositionCount(),
                "currentWarpPagePosition %s, currentWarpPage positions %s, warpPageRanges %s",
                currentWarpPagePosition,
                currentWarpSourcePage.getPositionCount(),
                warpPageRanges);
        if (currentWarpPagePosition == currentWarpSourcePage.getPositionCount()) {
            checkState(
                    warpPageRanges.isEmpty(),
                    "currentWarpPagePosition %s, currentWarpPage positions %s, warpPageRanges %s",
                    currentWarpPagePosition,
                    currentWarpSourcePage.getPositionCount(),
                    warpPageRanges);
            getNextWarpSourcePage();
        }
    }

    /**
     * The proxied range queue holds at most the unconsumed tail of the current proxied page.
     */
    private void skipCurrentProxiedPage(long rowsToSkip)
    {
        if (rowsToSkip != currentProxiedPage.getPositionCount() - currentProxiedPagePosition) {
            throw new TrinoException(
                    WarpErrorCode.WARP_PROXIED_ROW_MISMATCH,
                    format("rowsToSkip %s, currentProxiedPage positions %s, currentProxiedPagePosition %s", rowsToSkip, currentProxiedPage.getPositionCount(), currentProxiedPagePosition));
        }
        getNextProxiedPage();
    }

    private SourcePage buildMergedPage(Page overlapProxiedPage, Page overlapWarpPage, int positionCount)
    {
        if (queryContext.getTotalCollectCount() != (currentProxiedPage.getChannelCount() + currentWarpSourcePage.getChannelCount() + prefilledPageSource.getChannelCount())) {
            throw new TrinoException(WarpErrorCode.WARP_FAILED_TO_BUILD_MIXED_PAGE, "wrong number of columns");
        }
        Block[] orderedBlocks = new Block[queryContext.getTotalCollectCount()];
        for (int proxiedIndex = 0; proxiedIndex < proxiedOutputChannels.length; proxiedIndex++) {
            orderedBlocks[proxiedOutputChannels[proxiedIndex]] = overlapProxiedPage.getBlock(proxiedIndex);
        }
        for (int channel : prefilledOutputChannels) {
            orderedBlocks[channel] = prefilledPageSource.createBlock(channel, positionCount);
        }
        for (int warpIndex = 0; warpIndex < warpOutputChannels.length; warpIndex++) {
            orderedBlocks[warpOutputChannels[warpIndex]] = overlapWarpPage.getBlock(warpIndex);
        }
        return SourcePage.create(new Page(positionCount, orderedBlocks));
    }

    /**
     * Merges eagerly resolved warp and prefilled blocks with proxied channels that load on demand, so the
     * engine's filter short-circuit can skip decoding proxied columns of pages it fully filters out.
     */
    private SourcePage buildLazyMergedPage(SourcePage maskedProxiedPage, Page overlapWarpPage, int positionCount)
    {
        if (queryContext.getTotalCollectCount() != (maskedProxiedPage.getChannelCount() + currentWarpSourcePage.getChannelCount() + prefilledPageSource.getChannelCount())) {
            throw new TrinoException(WarpErrorCode.WARP_FAILED_TO_BUILD_MIXED_PAGE, "wrong number of columns");
        }
        Block[] eagerBlocks = new Block[queryContext.getTotalCollectCount()];
        for (int channel : prefilledOutputChannels) {
            eagerBlocks[channel] = prefilledPageSource.createBlock(channel, positionCount);
        }
        for (int warpIndex = 0; warpIndex < warpOutputChannels.length; warpIndex++) {
            eagerBlocks[warpOutputChannels[warpIndex]] = overlapWarpPage.getBlock(warpIndex);
        }
        return new MergedSourcePage(
                eagerBlocks,
                maskedProxiedPage,
                proxiedIndexByOutputChannel,
                positionCount,
                loadNanos -> {
                    stats.addproxied_loaded_pages_time(loadNanos);
                    recordProxiedPageLoad(maskedProxiedPage);
                });
    }

    /**
     * Resolves the warp matches of the current proxied page and masks it down to the matched positions with
     * {@link SourcePage#selectPositions}. When the current warp page ends inside the proxied page, the resolved
     * prefix is emitted as a smaller page and the unresolved tail stays selected for the next warp page;
     * MergePages coalesces small pages downstream.
     */
    private SourcePage mergeCurrentProxiedPage()
    {
        RowRange proxiedPageRange = proxiedPageRanges.removeFirst();
        long pageStartInclusive = proxiedPageRange.minInclusive();
        long pageEndExclusive = proxiedPageRange.maxExclusive();
        int[] positions = new int[toIntExact(pageEndExclusive - pageStartInclusive)];
        int positionCount = 0;
        int warpRowsToSkip = 0;
        long firstUnresolvedRow = pageStartInclusive;
        boolean pageResolved = false;
        while (!warpPageRanges.isEmpty()) {
            RowRange warpRange = warpPageRanges.peekFirst();
            if (warpRange.minInclusive() >= pageEndExclusive) {
                pageResolved = true;
                break;
            }
            warpPageRanges.removeFirst();
            if (warpRange.maxExclusive() <= pageStartInclusive) {
                warpRowsToSkip += toIntExact(warpRange.getRowCount());
                continue;
            }
            warpRowsToSkip += toIntExact(Math.max(pageStartInclusive - warpRange.minInclusive(), 0));
            long matchedStartInclusive = Math.max(warpRange.minInclusive(), pageStartInclusive);
            long matchedEndExclusive = Math.min(warpRange.maxExclusive(), pageEndExclusive);
            for (long row = matchedStartInclusive; row < matchedEndExclusive; row++) {
                positions[positionCount] = currentProxiedPagePosition + toIntExact(row - pageStartInclusive);
                positionCount++;
            }
            firstUnresolvedRow = matchedEndExclusive;
            if (warpRange.maxExclusive() > pageEndExclusive) {
                warpPageRanges.addFirst(new RowRange(pageEndExclusive, warpRange.maxExclusive()));
                pageResolved = true;
                break;
            }
        }
        int warpPagePosition = currentWarpPagePosition + warpRowsToSkip;
        if (!pageResolved && !warpPageSource.isFinished() && firstUnresolvedRow < pageEndExclusive) {
            // Current warp page ended inside the proxied page; emit the resolved prefix and keep the
            // unresolved tail selected so the next warp page can resolve it
            int resolvedRowCount = positionCount;
            for (long row = firstUnresolvedRow; row < pageEndExclusive; row++) {
                positions[positionCount] = currentProxiedPagePosition + toIntExact(row - pageStartInclusive);
                positionCount++;
            }
            maskCurrentProxiedPage(positions, positionCount);
            // The head warp range overlaps this page, so at least one position is already resolved
            long loadStart = System.nanoTime();
            Page resolvedProxiedPage = currentProxiedPage.getPage().getRegion(0, resolvedRowCount);
            stats.addproxied_loaded_pages_time(System.nanoTime() - loadStart);
            recordProxiedPageLoad(currentProxiedPage);
            Page resolvedWarpPage = currentWarpSourcePage.getPage().getRegion(warpPagePosition, resolvedRowCount);
            SourcePage mergedPage = buildMergedPage(resolvedProxiedPage, resolvedWarpPage, resolvedRowCount);
            proxiedPageRanges.addFirst(new RowRange(firstUnresolvedRow, pageEndExclusive));
            currentProxiedPagePosition = resolvedRowCount;
            seekWarpPageSource(warpRowsToSkip + resolvedRowCount);
            return mergedPage;
        }
        maskCurrentProxiedPage(positions, positionCount);
        Page overlapWarpPage = currentWarpSourcePage.getPage().getRegion(warpPagePosition, positionCount);
        SourcePage mergedPage = buildLazyMergedPage(currentProxiedPage, overlapWarpPage, positionCount);
        seekWarpPageSource(warpRowsToSkip + positionCount);
        // The merged page loads proxied channels on demand; the next proxied page is fetched lazily
        // on the next call, after the engine has consumed this one
        currentProxiedPage = null;
        return mergedPage;
    }

    /**
     * The positions are ascending and start at {@code currentProxiedPagePosition}, so a count equal to the
     * page size means every position is kept and masking would copy each block onto itself.
     */
    private void maskCurrentProxiedPage(int[] positions, int positionCount)
    {
        if (positionCount == currentProxiedPage.getPositionCount()) {
            return;
        }
        currentProxiedPage.selectPositions(positions, 0, positionCount);
    }

    /**
     * A lazy merged page can load its proxied blocks after the next proxied page was already fetched,
     * so page counts are deduplicated by page identity. {@link SourcePage#getSizeInBytes} covers only
     * loaded blocks, so bytes are recorded as its growth since the previous load of the same page.
     */
    private void recordProxiedPageLoad(SourcePage proxiedPage)
    {
        if (!isSameSourcePage(lastLoadRecordedProxiedPage, proxiedPage)) {
            lastLoadRecordedProxiedPage = proxiedPage;
            lastLoadRecordedProxiedPageSizeInBytes = 0;
            stats.incproxied_loaded_pages();
        }
        long sizeInBytes = proxiedPage.getSizeInBytes();
        stats.addproxied_loaded_pages_bytes(Math.max(0, sizeInBytes - lastLoadRecordedProxiedPageSizeInBytes));
        lastLoadRecordedProxiedPageSizeInBytes = sizeInBytes;
    }

    @SuppressWarnings("ReferenceEquality")
    private static boolean isSameSourcePage(SourcePage left, SourcePage right)
    {
        return left == right;
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
        long readTimeNanos = warpPageSource.getReadTimeNanos();
        if (proxiedConnectorPageSource != null) {
            readTimeNanos += proxiedConnectorPageSource.getReadTimeNanos();
        }
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return warpPageRanges.isEmpty() && warpPageSource.isFinished();
    }

    @Override
    @SuppressWarnings("deprecation") // TODO (https://github.com/trinodb/trino/issues/29959) migrate to MemoryContext
    public long getMemoryUsage()
    {
        return warpPageSource.getMemoryUsage() + (proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getMemoryUsage());
    }

    @Override
    public void close()
            throws IOException
    {
        // don't call prefilledPageSource.close since it is not used as a ConnectorPageSource here
        try {
            Exception closeException = null;
            try {
                warpPageRanges.clear();
                currentWarpSourcePage = null;
                warpPageSource.close();
            }
            catch (Exception e) {
                closeException = e;
            }
            try {
                if (proxiedConnectorPageSource != null) {
                    proxiedPageRanges.clear();
                    currentProxiedPage = null;
                    lastLoadRecordedProxiedPage = null;
                    proxiedConnectorPageSource.close();
                }
            }
            catch (Exception e) {
                if (closeException == null) {
                    closeException = e;
                }
                else {
                    closeException.addSuppressed(e);
                }
            }
            if (closeException == null) {
                stats.inccached_warp_success_files();
            }
            else {
                stats.inccached_warp_failed_files();
                throw new IOException(format("failed to close file %s", rowGroupData.getRowGroupKey()), closeException);
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
            proxiedConnectorPageSource = proxiedConnectorPageSourceProvider.get();
        }
        currentProxiedPage = null;
        while (currentProxiedPage == null && !proxiedConnectorPageSource.isFinished()) {
            currentProxiedPage = proxiedConnectorPageSource.getNextSourcePage();
            if (currentProxiedPage != null && currentProxiedPage.getPositionCount() > 0) {
                proxiedPageRanges.add(new RowRange(proxiedPagePositionsRead, proxiedPagePositionsRead + currentProxiedPage.getPositionCount()));
            }
        }
        currentProxiedPagePosition = 0;
        if (currentProxiedPage == null) {
            // proxiedConnectorPageSource is finished, return an empty page
            currentProxiedPage = SourcePage.create(0);
        }
        else {
            proxiedPagePositionsRead += currentProxiedPage.getPositionCount();
            stats.incproxied_pages();
        }
        stats.addproxied_time(System.nanoTime() - start);
    }

    @VisibleForTesting
    PageSourceDecision getPageSourceDecision()
    {
        return pageSourceDecision;
    }

    private record RowRange(long minInclusive, long maxExclusive)
    {
        RowRange
        {
            checkArgument(
                    minInclusive < maxExclusive && minInclusive >= 0,
                    "minInclusive %s must be smaller than maxExclusive %s",
                    minInclusive,
                    maxExclusive);
        }

        boolean isFullyBefore(RowRange other)
        {
            return maxExclusive <= other.minInclusive();
        }

        long getRowCount()
        {
            return maxExclusive - minInclusive;
        }
    }

    // TODO add @FormatMethod after implementing ConnectorPageSource.RowRanges#toString
    @SuppressWarnings("AnnotateFormatMethod")
    // @FormatMethod
    private static void validateRanges(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new TrinoException(WarpErrorCode.WARP_MATCH_RANGES_ERROR, format(formatString, args));
        }
    }

    /**
     * Output page over eagerly resolved blocks and a backing page that supplies the remaining channels on
     * first access; a null entry in {@code blocks} loads from the backing page through
     * {@code backingIndexByOutputChannel}.
     */
    private static class MergedSourcePage
            implements SourcePage
    {
        private final Block[] blocks;
        private final SourcePage backingPage;
        private final int[] backingIndexByOutputChannel;
        private final LongConsumer backingLoadListener;
        private int positionCount;

        MergedSourcePage(
                Block[] blocks,
                SourcePage backingPage,
                int[] backingIndexByOutputChannel,
                int positionCount,
                LongConsumer backingLoadListener)
        {
            this.blocks = blocks;
            this.backingPage = backingPage;
            this.backingIndexByOutputChannel = backingIndexByOutputChannel;
            this.positionCount = positionCount;
            this.backingLoadListener = backingLoadListener;
        }

        @Override
        public int getPositionCount()
        {
            return positionCount;
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
            // backingPage is retained even before any load; blocks loaded from it are views of the same memory
            long retainedSizeInBytes = backingPage.getRetainedSizeInBytes();
            for (int channel = 0; channel < blocks.length; channel++) {
                if (blocks[channel] != null && backingIndexByOutputChannel[channel] < 0) {
                    retainedSizeInBytes += blocks[channel].getRetainedSizeInBytes();
                }
            }
            return retainedSizeInBytes;
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            // backingPage is retained even before any load; blocks loaded from it are views of the same memory
            backingPage.retainedBytesForEachPart(consumer);
            for (int channel = 0; channel < blocks.length; channel++) {
                if (blocks[channel] != null && backingIndexByOutputChannel[channel] < 0) {
                    blocks[channel].retainedBytesForEachPart(consumer);
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
                loadBlocks(new int[] {channel});
            }
            return blocks[channel];
        }

        private void loadBlocks(int[] channels)
        {
            int missingCount = 0;
            for (int channel : channels) {
                if (blocks[channel] == null) {
                    missingCount++;
                }
            }
            if (missingCount == 0) {
                return;
            }
            int[] missingChannels = new int[missingCount];
            int[] backingChannels = new int[missingCount];
            int index = 0;
            for (int channel : channels) {
                if (blocks[channel] == null) {
                    missingChannels[index] = channel;
                    backingChannels[index] = backingIndexByOutputChannel[channel];
                    index++;
                }
            }
            // Single getColumns call loads all missing blocks in one pass over the backing page
            long loadStart = System.nanoTime();
            Page backingBlocks = backingPage.getColumns(backingChannels);
            for (int i = 0; i < missingCount; i++) {
                blocks[missingChannels[i]] = backingBlocks.getBlock(i);
            }
            // notified after the load so the listener sees the materialized bytes
            backingLoadListener.accept(System.nanoTime() - loadStart);
        }

        @Override
        public Page getPage()
        {
            int[] allChannels = new int[blocks.length];
            for (int channel = 0; channel < blocks.length; channel++) {
                allChannels[channel] = channel;
            }
            loadBlocks(allChannels);
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public Page getColumns(int[] channels)
        {
            loadBlocks(channels);
            Block[] projectedBlocks = new Block[channels.length];
            for (int i = 0; i < channels.length; i++) {
                projectedBlocks[i] = blocks[channels[i]];
            }
            return new Page(getPositionCount(), projectedBlocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            for (int i = 0; i < blocks.length; i++) {
                if (blocks[i] != null) {
                    blocks[i] = blocks[i].getPositions(positions, offset, size);
                }
            }
            backingPage.selectPositions(positions, offset, size);
            positionCount = size;
        }

        @Override
        public boolean trySelectPositions(int[] positions, int offset, int size)
        {
            if (!backingPage.trySelectPositions(positions, offset, size)) {
                return false;
            }
            // the caller owns the positions array and dictionary views retain it, so loaded blocks get a copy
            int[] retainedPositions = null;
            for (int i = 0; i < blocks.length; i++) {
                Block block = blocks[i];
                if (block == null) {
                    continue;
                }
                if (backingIndexByOutputChannel[i] >= 0) {
                    // the backing page already narrowed its loaded blocks, so take its view instead of wrapping the stale full block
                    blocks[i] = backingPage.getBlock(backingIndexByOutputChannel[i]);
                    continue;
                }
                if (retainedPositions == null) {
                    retainedPositions = Arrays.copyOfRange(positions, offset, offset + size);
                }
                blocks[i] = block.getPositions(retainedPositions, 0, size);
            }
            positionCount = size;
            return true;
        }
    }
}
