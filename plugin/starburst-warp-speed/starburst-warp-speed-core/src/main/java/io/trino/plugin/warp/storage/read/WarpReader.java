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
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static java.util.Objects.requireNonNull;

public class WarpReader
{
    private static final Logger logger = Logger.get(WarpReader.class);
    // The heap size of a worker node on galaxy is 80GB. The total off heap memory limit we take here is 256KB * 64 threads equals 16MB.
    // 1 percentage of the heap size is 800MB, so these 16MB is much less than 1 percentage. It means we are guaranteed the GC will
    // not be blocked by this small off heap memory.
    // The limit check is to make sure we do not accidentally enlarge the off heap allocation
    private static final long LIMIT_OFF_HEAP_MEMORY = 256 * 1024;

    // parameters
    private final ReadTimeMeasurement readTimeMeasurement;
    private final ShapingLogger shapingLogger;
    private final QueryArgs queryArgs;
    private final AggregatorArgs aggregatorArgs;
    private final MatcherArgs matcherArgs;
    private final StorageCollectorService storageCollectorService;
    private final Matcher matcher;

    private final WarpQueryState queryState;
    private AggregatorPageArgs aggregatorPageArgs;
    private MatcherPageArgs matcherPageArgs;
    private final long rowsLimit;

    WarpReader(QueryParams queryParams,
            CustomStatsContext customStatsContext,
            StorageCollectorService storageCollectorService,
            Matcher matcher,
            GlobalConfig globalConfig,
            long rowsLimit)
    {
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.matcher = requireNonNull(matcher);
        this.rowsLimit = rowsLimit;

        this.queryArgs = storageCollectorService.getQueryArgs(queryParams, customStatsContext);
        this.aggregatorArgs = storageCollectorService.open(queryArgs);
        this.matcherArgs = matcher.open(queryArgs, customStatsContext);
        queryState = new WarpQueryState();

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());

        checkOffHeapMemoryUsage();
        readTimeMeasurement = new ReadTimeMeasurement();
    }

    public boolean isRowsLimitReached()
    {
        return rowsLimit <= queryState.getTotalNumReadRecords();
    }

    void close()
    {
        storageCollectorService.close(queryArgs);
    }

    // verify total amount of off heap memory allocated does not exceed a limit
    private void checkOffHeapMemoryUsage()
    {
        long totalOffHeapSize = storageCollectorService.getOffHeapMemoryUsage(aggregatorArgs) +
                matcher.getOffHeapMemoryUsage(matcherArgs);

        if (totalOffHeapSize > LIMIT_OFF_HEAP_MEMORY) {
            shapingLogger.warn("off heap memory exceeded threshold " + totalOffHeapSize);
        }
    }

    /**
     * prepare buffers for filling
     */
    @NativeInterrupt
    private void openPage()
    {
        int pageLimit = (int) Math.min(rowsLimit - queryState.getTotalNumReadRecords(), Integer.MAX_VALUE);

        aggregatorPageArgs = storageCollectorService.openPage(queryArgs, aggregatorArgs, queryState, pageLimit);
        try {
            matcherPageArgs = matcher.openPage(queryArgs, matcherArgs, aggregatorPageArgs);
        }
        catch (Exception e) {
            throw new TrinoException(WARP_MATCH_FAILED, "failed to open match");
        }
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean prepareBlocks()
    {
        long startTime = readTimeMeasurement.getStartTime();

        if (aggregatorPageArgs == null) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }

        // we continue as long as we didn't reach a limit from the match nor prepare
        while (matcher.match(queryArgs, matcherArgs, matcherPageArgs)) {
            if (!storageCollectorService.prepareBlocks(queryArgs,
                    aggregatorArgs,
                    aggregatorPageArgs,
                    queryState)) {
                break;
            }
        }

        readTimeMeasurement.updateRuntimeMeasurements(startTime, queryArgs);
        return queryState.getNumRecordsInCurPage() > 0;
    }

    ReadResult getPage()
    {
        try {
            Block[] blocks = new Block[0];
            WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;

            openPage();
            if (prepareBlocks()) {
                if (queryState.getNumRecordsInCurPage() > rowsLimit - queryState.getTotalNumReadRecords()) {
                    shapingLogger.warn("numRecordsInCurPage is exceeding the limit. numRecordsInCurPage %d totalNumReadRecords %d rowsLimit %d page limit %d",
                            queryState.getNumRecordsInCurPage(),
                            queryState.getTotalNumReadRecords(),
                            rowsLimit,
                            rowsLimit - queryState.getTotalNumReadRecords());
                }

                blocks = storageCollectorService.aggregateBlocks(queryArgs,
                        aggregatorArgs,
                        queryState);
                if (queryArgs.queryParams().isRangesRequired()) {
                    ranges = storageCollectorService.collectRanges(aggregatorPageArgs);
                }
            }
            long numReadPages = closePage();

            return new ReadResult(blocks, queryState.getNumRecordsInCurPage(), ranges, numReadPages);
        }
        catch (Exception e) {
            abortPage(e);
            throw e;
        }
    }

    @NativeInterrupt
    private long closePage()
    {
        if (matcherPageArgs != null) {
            matcher.closePage(queryArgs, matcherPageArgs);
            matcherPageArgs = null;
        }

        if (aggregatorPageArgs == null) {
            return 0;
        }

        queryState.addTotalNumReadRecords(queryState.getNumRecordsInCurPage());
        long readPages = storageCollectorService.closePage(queryArgs,
                aggregatorPageArgs,
                aggregatorArgs,
                queryState);

        aggregatorPageArgs = null;
        return readPages;
    }

    private void abortPage(Exception e)
    {
        if (matcherPageArgs != null) {
            matcher.abortPage(queryArgs, matcherPageArgs, e);
            matcherPageArgs = null;
        }
        if (aggregatorPageArgs != null) {
            storageCollectorService.abortPage(queryArgs, aggregatorPageArgs, e);
            aggregatorPageArgs = null;
        }
    }
}
