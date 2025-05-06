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

import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.ObjLongConsumer;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static java.util.Objects.requireNonNull;

public class WarpReader
{
    // parameters
    private final ShapingLogger shapingLogger;
    private final WorkerMemoryManager workerMemoryManager;
    private final QueryArgs queryArgs;
    private final WarpQueryState queryState;
    private final long rowsLimit;
    private ThreadArena pageArena;

    private final AggregatorArgs aggregatorArgs;
    private final BlocksAggregator blocksAggregator;
    private AggregatorPageArgs aggregatorPageArgs;

    private final Matcher matcher;
    private final MatcherArgs matcherArgs;
    private MatcherPageArgs matcherPageArgs;

    WarpReader(QueryParams queryParams,
            CustomStatsContext customStatsContext,
            BlocksAggregator blocksAggregator,
            Matcher matcher,
            WorkerMemoryManager workerMemoryManager,
            ShapingLoggerFactory shapingLoggerFactory,
            long rowsLimit)
    {
        this.workerMemoryManager = requireNonNull(workerMemoryManager);
        this.blocksAggregator = requireNonNull(blocksAggregator);
        this.matcher = requireNonNull(matcher);
        this.rowsLimit = rowsLimit;
        this.queryArgs = blocksAggregator.getQueryArgs(queryParams, customStatsContext);
        this.aggregatorArgs = blocksAggregator.open(queryArgs);
        this.matcherArgs = matcher.open(queryArgs, customStatsContext);

        this.queryState = new WarpQueryState();

        this.shapingLogger = shapingLoggerFactory.getInstance(WarpReader.class);
    }

    public boolean isRowsLimitReached()
    {
        return rowsLimit <= queryState.getTotalNumReadRecords();
    }

    void close()
    {
        blocksAggregator.close(queryArgs);
    }

    /**
     * prepare buffers for filling
     */
    private void openPage(List<Integer> blocksToLoad, RecordIndexes recordIndexes)
    {
        queryState.resetNumRecordsInCurPage();
        pageArena = workerMemoryManager.getThreadArena();

        // each API call will throw exception if failed
        aggregatorPageArgs = blocksAggregator.openPage(recordIndexes,
                queryArgs,
                pageArena,
                aggregatorArgs,
                queryState,
                blocksToLoad);

        matcherPageArgs = matcher.openPage(recordIndexes,
                pageArena,
                queryArgs,
                matcherArgs,
                aggregatorPageArgs);
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    private boolean prepareBlocks(List<Integer> blocksToLoad, RecordIndexes recordIndexes, List<ChunkProperties> pageChunksList)
    {
        if (aggregatorPageArgs == null) {
            throw new TrinoException(WARP_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }

        // we continue as long as we didn't reach the rowsLimit nor a limit from the matcher or aggregator
        int recordsPageLimit = (int) Math.min(aggregatorArgs.getAggregatorPageLimit(), (rowsLimit - queryState.getTotalNumReadRecords()));

        while (queryState.getNumRecordsInCurPage() < recordsPageLimit) {
            Optional<ChunkProperties> chunk = matcher.match(recordsPageLimit, queryArgs, matcherArgs, matcherPageArgs, queryState);
            if (chunk.isEmpty()) {
                break;
            }
            blocksAggregator.prepareBlocks(chunk.get(),
                    recordIndexes,
                    queryArgs,
                    aggregatorArgs,
                    aggregatorPageArgs,
                    queryState,
                    blocksToLoad);
            pageChunksList.add(chunk.get());
            queryArgs.dispatcherPageSourceStats().addcached_read_rows(chunk.get().numRecordsInChunk());
        }

        return queryState.getNumRecordsInCurPage() > 0;
    }

    ReadResult getSourcePage()
    {
        ReadResult readResult;
        try {
            Block[] blocks = new Block[queryArgs.queryParams().getNumCollectElements()];
            WarpStoragePageSource.RowRanges ranges = WarpStoragePageSource.RowRanges.EMPTY;
            RecordIndexes recordIndexes = new RecordIndexes(queryArgs.chunkSize());
            List<ChunkProperties> pageChunksList = new ArrayList<>();
            List<Integer> preLoadedBlocks = blocksAggregator.getPreLoadedBlocks(queryArgs);
            openPage(preLoadedBlocks, recordIndexes);
            if (prepareBlocks(preLoadedBlocks, recordIndexes, pageChunksList)) {
                if (queryState.getNumRecordsInCurPage() > rowsLimit - queryState.getTotalNumReadRecords()) {
                    shapingLogger.warn("numRecordsInCurPage is exceeding the limit. numRecordsInCurPage %d totalNumReadRecords %d rowsLimit %d page limit %d",
                            queryState.getNumRecordsInCurPage(),
                            queryState.getTotalNumReadRecords(),
                            rowsLimit,
                            rowsLimit - queryState.getTotalNumReadRecords());
                }

                blocksAggregator.aggregateBlocks(recordIndexes,
                        queryArgs,
                        aggregatorArgs,
                        aggregatorPageArgs,
                        queryState,
                        pageChunksList,
                        preLoadedBlocks,
                        blocks);
                if (queryArgs.queryParams().isRangesRequired()) {
                    ranges = matcher.getRanges(matcherPageArgs);
                }
                queryArgs.dispatcherPageSourceStats().addlazy_collect_total_blocks(blocks.length - preLoadedBlocks.size());
            }
            long numReadPages = closePage();

            WarpSourcePage warpSourcePage = new WarpSourcePage(recordIndexes, pageChunksList, queryState.getNumRecordsInCurPage(), blocks);
            readResult = new ReadResult(warpSourcePage, queryState.getNumRecordsInCurPage(), ranges, numReadPages);
        }
        catch (Exception e) {
            abortPage(e);
            throw e;
        }
        return readResult;
    }

    private long closePage()
    {
        long readPages = 0;

        try {
            if (matcherPageArgs != null) {
                matcher.closePage(queryArgs, matcherArgs, matcherPageArgs);
            }

            if (aggregatorPageArgs != null) {
                queryState.addTotalNumReadRecords(queryState.getNumRecordsInCurPage());
                readPages = blocksAggregator.closePage(queryArgs, aggregatorPageArgs);
            }

            if (pageArena != null) {
                pageArena.close();
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to close page");
        }
        finally {
            pageArena = null;
            matcherPageArgs = null;
            aggregatorPageArgs = null;
        }

        return readPages;
    }

    private void abortPage(Exception e)
    {
        try {
            if (matcherPageArgs != null) {
                matcher.abortPage(queryArgs, matcherPageArgs, e);
            }
            if (aggregatorPageArgs != null) {
                blocksAggregator.abortPage(queryArgs, aggregatorPageArgs, e);
            }
            if (pageArena != null) {
                pageArena.close();
            }
        }
        catch (Exception e2) {
            shapingLogger.error(e2, "failed to abort page");
        }
        finally {
            pageArena = null;
            matcherPageArgs = null;
            aggregatorPageArgs = null;
        }
    }

    public class WarpSourcePage
            implements SourcePage
    {
        private final RecordIndexes recordIndexes;
        private final List<ChunkProperties> chunkPropertiesList;
        private final int positionCount;
        private final Block[] blocks;

        public WarpSourcePage(RecordIndexes recordIndexes,
                List<ChunkProperties> chunkPropertiesList,
                int positionCount,
                Block[] blocks)
        {
            this.recordIndexes = recordIndexes;
            this.chunkPropertiesList = chunkPropertiesList;
            this.positionCount = positionCount;
            this.blocks = blocks;
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

        void loadBlocks(List<Integer> blocksToLoad)
        {
            blocksToLoad = blocksToLoad.stream()
                    .filter(i -> blocks[i] == null)
                    .toList();
            if (blocksToLoad.isEmpty()) {
                return;
            }

            try {
                pageArena = workerMemoryManager.getThreadArena();
                aggregatorPageArgs = blocksAggregator.openPage(recordIndexes,
                        queryArgs,
                        pageArena,
                        aggregatorArgs,
                        queryState,
                        blocksToLoad);

                for (ChunkProperties chunk : chunkPropertiesList) {
                    blocksAggregator.prepareBlocks(chunk,
                            recordIndexes,
                            queryArgs,
                            aggregatorArgs,
                            aggregatorPageArgs,
                            queryState,
                            blocksToLoad);
                }

                blocksAggregator.aggregateBlocks(recordIndexes,
                        queryArgs,
                        aggregatorArgs,
                        aggregatorPageArgs,
                        queryState,
                        chunkPropertiesList,
                        blocksToLoad,
                        blocks);
            }
            catch (Exception e) {
                abortPage(e);
                throw e;
            }
            closePage();
            queryArgs.dispatcherPageSourceStats().addlazy_collect_loaded_blocks(blocksToLoad.size());
        }

        @Override
        public Block getBlock(int channel)
        {
            loadBlocks(List.of(channel));
            return blocks[channel];
        }

        @Override
        public Page getPage()
        {
            if (positionCount > 0) {
                List<Integer> blocksToLoad = IntStream.range(0, blocks.length)
                        .boxed()
                        .toList();
                loadBlocks(blocksToLoad);
            }
            return blocks.length > 0 ? new Page(blocks) : new Page(positionCount);
        }

        @Override
        public Page getColumns(int[] channels)
        {
            List<Integer> blocksToLoad = Arrays.stream(channels)
                    .boxed()
                    .toList();
            loadBlocks(blocksToLoad);
            Block[] blocks = new Block[channels.length];
            for (int i = 0; i < channels.length; i++) {
                blocks[i] = getBlock(channels[i]);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            // TODO: implement lazy selectPositions
            for (int i = 0; i < blocks.length; i++) {
                if (blocks[i] == null) {
                    blocks[i] = getBlock(i);
                }
                blocks[i] = blocks[i].getPositions(positions, offset, size);
            }
        }
    }
}
