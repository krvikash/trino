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

import java.util.Optional;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static java.util.Objects.requireNonNull;

public class StorageReader
{
    private static final Logger logger = Logger.get(StorageReader.class);
    // The heap size of a worker node on galaxy is 80GB. The total off heap memory limit we take here is 256KB * 64 threads equals 16MB.
    // 1 prefcentage of the heap size is 800MB, so these 16MB is much less than 1 precentage. It means we are guaranteed the GC will
    // not be blocked by this small off heap memory.
    // The limit check is to make sure we do not accidently enlarge the off heap allocation
    private static final long LIMIT_OFF_HEAP_MEMORY = 256 * 1024;

    // parameters
    private final ReadTimeMeasurement readTimeMeasurement;
    private final ShapingLogger shapingLogger;
    private final QueryArgs queryArgs;
    private final StorageCollectorArgs storageCollectorArgs;
    private final MatchArgs matchArgs;
    private final StorageCollectorService storageCollectorService;
    private final MatchService matchService;

    private final WarpQueryState queryState;
    private Optional<StoreRowListResult> storeRowListResult;
    private CollectOpenResult collectOpenResult;
    private MatchOpenResult matchOpenResult;

    StorageReader(QueryParams queryParams,
            CustomStatsContext customStatsContext,
            StorageCollectorService storageCollectorService,
            MatchService matchService,
            GlobalConfig globalConfig)
    {
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.matchService = requireNonNull(matchService);

        this.queryArgs = storageCollectorService.getQueryArgs(queryParams, customStatsContext);
        this.storageCollectorArgs = storageCollectorService.getStorageCollectorArgs(queryArgs);
        this.storeRowListResult = Optional.empty();
        storageCollectorService.init(queryArgs);
        this.matchArgs = matchService.init(queryArgs, customStatsContext);
        queryState = new WarpQueryState();

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());

        checkOffHeapMemoryUsage();
        readTimeMeasurement = new ReadTimeMeasurement();
    }

    void close()
    {
        storageCollectorService.terminate(queryArgs);
    }

    // verify total amount of off heap memory allocated does not exceed a limit
    private void checkOffHeapMemoryUsage()
    {
        if ((queryArgs != null) && (storageCollectorArgs != null) && (matchArgs != null)) {
            long totalOffHeapSize = queryArgs.txArgs().collectStateBuff().byteSize() +
                    storageCollectorArgs.recordBufferStates().byteSize() +
                    storageCollectorArgs.recordIndexes().byteSize() +
                    storageCollectorArgs.queryResultTypes().byteSize() +
                    storageCollectorArgs.prepareQueryResultTypes().byteSize() +
                    storageCollectorArgs.warmUpElementAtts().byteSize() +
                    matchArgs.warmUpElementAtts().byteSize();
            if (totalOffHeapSize > LIMIT_OFF_HEAP_MEMORY) {
                shapingLogger.warn("off heap memory exceeded threshold " + totalOffHeapSize);
            }
        }
    }

    /**
     * prepare buffers for filling
     */
    @NativeInterrupt
    private void queryOpen(int rowsLimit)
    {
        collectOpenResult = storageCollectorService.open(queryArgs, storageCollectorArgs, queryState, rowsLimit, storeRowListResult);
        try {
            matchOpenResult = matchService.open(queryArgs, matchArgs, collectOpenResult);
        }
        catch (Exception e) {
            throw new TrinoException(WARP_MATCH_FAILED, "failed to open match");
        }
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean matchAndCollect()
    {
        long startTime = readTimeMeasurement.getStartTime();

        if (collectOpenResult == null) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }
        CollectBufferState collectBufferState = CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;

        // we continue as long as buffer is not full and we have more chunks to match and collect
        while ((collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_FULL) && matchService.match(queryArgs, matchArgs, matchOpenResult)) {
            CollectFromStorageResult collectFromStorageResult = storageCollectorService.collectFromStorage(queryArgs,
                    storageCollectorArgs,
                    collectOpenResult,
                    queryState);
            collectBufferState = collectFromStorageResult.collectBufferState();
        }

        readTimeMeasurement.updateRuntimeMeasurements(startTime, queryArgs);
        return collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
    }

    private void fillBlocks(Block[] blocks)
    {
        queryState.setNumRecordsInCurPage(Math.min(queryState.getNumRecordsInCurPage(), collectOpenResult.rowsLimit()));

        storageCollectorService.fillBlocks(blocks,
                queryArgs,
                storageCollectorArgs,
                queryState);
    }

    ReadResult getPage(Block[] blocks, int limit)
    {
        try {
            WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;

            queryOpen(limit);
            if (matchAndCollect()) {
                fillBlocks(blocks);
                if (queryArgs.queryParams().isRangesRequired()) {
                    ranges = storageCollectorService.collectRanges(collectOpenResult);
                }
            }
            long numReadPages = queryClose();

            return new ReadResult(queryState.getNumRecordsInCurPage(), ranges, numReadPages);
        }
        catch (Exception e) {
            queryAbort(e);
            throw e;
        }
    }

    @NativeInterrupt
    private long queryClose()
    {
        if (matchOpenResult != null) {
            matchService.close(matchOpenResult, queryArgs.dispatcherPageSourceStats());
            matchOpenResult = null;
        }

        if (collectOpenResult == null) {
            return 0;
        }

        queryState.addTotalNumReadRecords(queryState.getNumRecordsInCurPage());
        CollectCloseResult collectCloseResult = storageCollectorService.close(queryArgs,
                collectOpenResult,
                storageCollectorArgs);
        collectOpenResult = null;
        storeRowListResult = collectCloseResult.storeRowListResult();
        return collectCloseResult.readPages();
    }

    private void queryAbort(Exception e)
    {
        if (matchOpenResult != null) {
            matchService.abort(matchOpenResult, e, queryArgs.dispatcherPageSourceStats());
            matchOpenResult = null;
        }
        if (collectOpenResult != null) {
            storageCollectorService.abort(collectOpenResult, e, queryArgs.dispatcherPageSourceStats());
            collectOpenResult = null;
        }
    }
}
