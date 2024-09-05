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
    // parameters
    private final ReadTimeMeasurement readTimeMeasurement;

    private final QueryArgs queryArgs;
    private final StorageCollectorArgs storageCollectorArgs;
    private final MatchArgs matchArgs;

    private final StorageCollectorService storageCollectorService;
    private final MatchService matchService;

    private int numRowsCollectedInCurRound; // num rows collected in this getNextPage
    private int numRowsCollectedInPrevRounds; // num rows collected in all previous getNextPages
    private int[] queryResultType;
    private Optional<StoreRowListResult> storeRowListResult;
    private CollectOpenResult collectOpenResult;
    private MatchOpenResult matchOpenResult;

    StorageReader(QueryParams queryParams,
            CustomStatsContext customStatsContext,
            StorageCollectorService storageCollectorService,
            MatchService matchService)
    {
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.matchService = requireNonNull(matchService);

        this.queryArgs = storageCollectorService.getQueryArgs(queryParams, customStatsContext);
        this.storageCollectorArgs = storageCollectorService.getStorageCollectorArgs(queryArgs);

        this.queryResultType = new int[queryParams.getNumCollectElements()];
        this.storeRowListResult = Optional.empty();

        storageCollectorService.init(queryArgs);

        this.matchArgs = matchService.init(queryArgs, customStatsContext);

        readTimeMeasurement = new ReadTimeMeasurement();
    }

    void close()
    {
        storageCollectorService.terminate(queryArgs);
    }

    /**
     * prepare buffers for filling
     */
    @NativeInterrupt
    private void queryOpen(int rowsLimit)
    {
        collectOpenResult = storageCollectorService.open(queryArgs, storageCollectorArgs, numRowsCollectedInPrevRounds, rowsLimit, storeRowListResult);
        try {
            matchOpenResult = matchService.open(queryArgs, matchArgs, collectOpenResult);
        }
        catch (Exception e) {
            throw new TrinoException(WARP_MATCH_FAILED, "failed to open match");
        }

        numRowsCollectedInCurRound = 0;
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
                    collectOpenResult,
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
                queryResultType);
        return rowsToFill;
    }

    ReadResult getPage(Block[] blocks, int limit)
    {
        try {
            int numCollectedRows = 0;
            WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;

            queryOpen(limit);
            if (matchAndCollect()) {
                numCollectedRows = fillBlocks(blocks);
                if (queryArgs.queryParams().isRangesRequired()) {
                    ranges = storageCollectorService.collectRanges(collectOpenResult);
                }
            }
            long numReadPages = queryClose();

            return new ReadResult(numCollectedRows, ranges, numReadPages);
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
                numRowsCollectedInCurRound);
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
