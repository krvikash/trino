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
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
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

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_NUM_OF;
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
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.collectTxService = requireNonNull(collectTxService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create());
        this.rangeFillerService = requireNonNull(rangeFillerService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.blockFillersFactory = requireNonNull(blockFillersFactory);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        shapingLogger = shapingLoggerFactory.getInstance(logger);
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
                aggregatorArgs);
    }

    void openChunk(int chunkIx,
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs)
    {
        collectTxService.openChunk(aggregatorPageArgs.collectState(),
                chunkIx,
                queryArgs.dispatcherPageSourceStats());
    }

    void collectChunk(AggregatorPageArgs aggregatorPageArgs,
            MemorySegment outQueryResultTypes,
            DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        collectTxService.collectChunk(aggregatorPageArgs.collectState(),
                outQueryResultTypes,
                dispatcherPageSourceStats);
    }

    // returns indication if we can continue preparing more records, or we reached some limit by the storage collector
    @NativeInterrupt
    public void prepareBlocks(ChunkProperties chunk,
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState)
    {
        int numCollectedRows = queryState.getNumRecordsInCurPage();
        RecordIndexes recordIndexes = aggregatorPageArgs.rangeData().getRecordIndexes();

        recordIndexes.setCurChunkProperties(chunk);
        if (queryArgs.queryParams().getNumCollectElements() > 0) {
            openChunk(chunk.chunkIndex(), queryArgs, aggregatorPageArgs);
            try {
                collectChunk(aggregatorPageArgs,
                        aggregatorPageArgs.queryResultTypes().get(),
                        queryArgs.dispatcherPageSourceStats());
                queryArgs.dispatcherPageSourceStats().addrecords_in_chunk(chunk.numRecordsInChunk());
            }
            catch (Exception e) {
                shapingLogger.error(e, "Failed To collect chunk %s numCollectedRows %d", chunk, numCollectedRows);
                throw e;
            }
        }
        numCollectedRows += rangeFillerService.add(chunk, queryArgs, aggregatorPageArgs, this);
        logger.debug("collectFromStorage after native collect current chunk %s numCollectedRows %d", chunk, numCollectedRows);

        queryState.setNumRecordsInCurPage(numCollectedRows);
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
            ReadJuffersWarmUpElement readJuffersWarmUpElement = aggregatorArgs.collectBuffersParams().collectJuffersWE().get(weIx);
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

        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();

        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }

        CollectBuffersParams collectBuffersParams = collectTxService.getCollectBuffersAllocationParams(queryParams);
        return new AggregatorArgs(blockFillers,
                collectBuffersParams);
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
            AggregatorPageArgs aggregatorPageArgs)
    {
        return collectTxService.collectStoreAndClose(queryArgs,
                aggregatorPageArgs);
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
