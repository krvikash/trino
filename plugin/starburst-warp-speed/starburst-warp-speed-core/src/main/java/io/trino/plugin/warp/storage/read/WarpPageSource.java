package io.trino.plugin.warp.storage.read;
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

import io.trino.plugin.warp.dispatcher.FilteringStats;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.spi.Page;
import io.trino.spi.connector.SourcePage;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Provide a PageSource API implemented with the helper class WarpStorageReader.
 */
public class WarpPageSource
        implements WarpStoragePageSource
{
    public static final int INVALID_COL_IX = -1;
    private final ShapingLogger shapingLogger;

    private final StorageEngineConstants storageEngineConstants;
    private final PredicatesCacheService predicatesCacheService;
    private final Optional<FilteringStats> filteringStats;
    private final QueryParams queryParams;
    private final WarpReader reader;
    private boolean finished;
    private boolean closed; // set explicitly by someone calling {@link #close()}
    private RowRanges sortedRowRanges;
    private long completedBytes;
    private long completedPositions;

    public WarpPageSource(StorageEngineConstants storageEngineConstants,
            long rowsLimit,
            Optional<FilteringStats> filteringStats,
            QueryParams queryParams,
            PredicatesCacheService predicatesCacheService,
            CustomStatsContext customStatsContext,
            ShapingLoggerFactory shapingLoggerFactory,
            StorageCollectorService storageCollectorService,
            MatchService matchService,
            WorkerMemoryManager workerMemoryManager)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.predicatesCacheService = predicatesCacheService;
        this.filteringStats = filteringStats;
        this.sortedRowRanges = RowRanges.EMPTY;
        this.queryParams = queryParams;
        this.shapingLogger = shapingLoggerFactory.getInstance(WarpPageSource.class);
        reader = new WarpReader(
                queryParams,
                customStatsContext,
                storageCollectorService,
                matchService,
                workerMemoryManager,
                shapingLoggerFactory,
                rowsLimit);
    }

    @Override
    public long getMemoryUsage()
    {
        // TODO: https://expandb.atlassian.net/browse/VDB-4020
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public void close()
    {
        if (!closed) {
            filteringStats.ifPresent(stats -> stats.recordProcessed(queryParams.getTotalNumRecords(), completedPositions));
            closed = true;
            if (reader != null) {
                reader.close();
            }
            predicatesCacheService.markFinished(queryParams.getPredicateCacheData());
        }
    }

    @Override
    public boolean isFinished()
    {
        return finished || closed;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        Optional<SourcePage> sourcePage = Optional.empty();
        int currentPositionsCount = 0;
        if (!isFinished()) {
            try {
                ReadResult readResult = reader.getSourcePage();
                if (readResult.numCollectedRows() == 0) {
                    finished = true;
                }
                updateRowsLimit();
                sourcePage = Optional.of(readResult.sourcePage());
                sortedRowRanges = readResult.ranges();
                completedBytes += (readResult.numReadPages() << storageEngineConstants.getPageSizeShift());
                completedPositions += readResult.numCollectedRows();
            }
            catch (Exception e) {
                close();
                if (!Thread.currentThread().isInterrupted()) {
                    shapingLogger.error(e, "query failure filePath %s matchList %s collectList %s", queryParams.getFilePath(), queryParams.getMatchElementsParamsList(), queryParams.getCollectElementsParamsList());
                }
                throw e;
            }
        }

        // sourcePage.length can be 0 in case we just match in Warp when collect is done in external/prefilled
        return sourcePage.orElseGet(() -> SourcePage.create(new Page(currentPositionsCount)));
    }

    private void updateRowsLimit()
    {
        if (isRowsLimitReached()) {
            finished = true;
        }
    }

    @Override
    public RowRanges getSortedRowRanges()
    {
        return sortedRowRanges;
    }

    @Override
    public boolean isRowsLimitReached()
    {
        return reader.isRowsLimitReached();
    }
}
