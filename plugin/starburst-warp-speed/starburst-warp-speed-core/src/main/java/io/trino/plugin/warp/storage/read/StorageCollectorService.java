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
import io.trino.plugin.warp.config.GlobalConfig;
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
import io.trino.spi.catalog.CatalogName;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

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
    private final CollectTxService collectTxService;
    private final StorageEngineConstants storageEngineConstants;
    private final BlockFillersFactory blockFillersFactory;
    private final DictionaryCacheService dictionaryCacheService;

    private final ShapingLogger shapingLogger;
    private final ShapingLoggerFactory shapingLoggerFactory;
    private final LazyCollectTxService lazyCollectTxService;
    private final GlobalConfig globalConfig;
    private final CatalogName catalogName;

    @Inject
    StorageCollectorService(
            StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            CollectTxService collectTxService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory,
            DictionaryCacheService dictionaryCacheService,
            ShapingLoggerFactory shapingLoggerFactory,
            LazyCollectTxService lazyCollectTxService,
            GlobalConfig globalConfig,
            CatalogName catalogName)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.collectTxService = requireNonNull(collectTxService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create());
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.blockFillersFactory = requireNonNull(blockFillersFactory);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.shapingLogger = shapingLoggerFactory.getInstance(StorageCollectorService.class);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.lazyCollectTxService = lazyCollectTxService;
        this.globalConfig = globalConfig;
        this.catalogName = catalogName;
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

    public AggregatorPageArgs openPage(RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            ThreadArena pageArena,
            AggregatorArgs aggregatorArgs,
            WarpQueryState queryState)
    {
        queryState.resetNumRecordsInCurPage();
        return collectTxService.collectOpenAndRestore(recordIndexes,
                queryArgs,
                pageArena,
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

    @NativeInterrupt
    public void prepareBlocks(ChunkProperties chunk,
            RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState)
    {
        recordIndexes.setCurChunkProperties(chunk);
        if (queryArgs.queryParams().getNumCollectElements() > 0) {
            if (!aggregatorArgs.preLoadedCollectParamsList().isEmpty()) {
                openChunk(chunk.chunkIndex(), queryArgs, aggregatorPageArgs);
                try {
                    collectChunk(aggregatorPageArgs,
                            aggregatorPageArgs.queryResultTypes().get(),
                            queryArgs.dispatcherPageSourceStats());
                }
                catch (Exception e) {
                    shapingLogger.error(e, "Failed To collect chunk %s", chunk);
                    throw e;
                }
            }
        }
        logger.debug("collectFromStorage after native collect current chunk %s", chunk);
    }

    public Block[] aggregateBlocks(RecordIndexes recordIndexes,
            QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            AggregatorPageArgs aggregatorPageArgs,
            WarpQueryState queryState,
            List<ChunkProperties> pageChunksList)
    {
        List<WarmupElementCollectParams> collectElementsParamsList = queryArgs.queryParams().getCollectElementsParamsList();
        MemorySegment queryResultTypes = aggregatorPageArgs.queryResultTypes().orElse(MemorySegment.NULL);
        int rowsToFill = queryState.getNumRecordsInCurPage();
        Block[] blocks = new Block[collectElementsParamsList.size()];
        int preLoadedBlockIx = 0;

        for (int weIx = 0; weIx < collectElementsParamsList.size(); weIx++) {
            WarmupElementCollectParams collectParams = collectElementsParamsList.get(weIx);
            BlockFiller<?> blockFiller = aggregatorArgs.blockFillers().get(weIx);
            if (aggregatorArgs.preLoadedBlocks().get(weIx)) {
                ReadJuffersWarmUpElement readJuffersWarmUpElement = aggregatorArgs.collectBuffersParams().collectJuffersWE().get(preLoadedBlockIx);
                QueryResultType queryResultType = QueryResultType.values()[queryResultTypes.getAtIndex(ValueLayout.JAVA_INT, preLoadedBlockIx)];
                Block block = blockFiller.fillBlockWithRecords(collectParams, readJuffersWarmUpElement, rowsToFill, queryResultType, dictionaryStats, queryArgs.dispatcherPageSourceStats());
                blocks[collectParams.getBlockIndex()] = block;
                preLoadedBlockIx++;
            }
            else {
                QueryParams queryParams = queryArgs.queryParams();
                LazyCollectorLoaderArgs lazyCollectorLoaderArgs = new LazyCollectorLoaderArgs(queryParams,
                        queryArgs.fileCookie(),
                        collectParams,
                        new ReadJuffersWarmUpElement(bufferAllocator, true),
                        blockFiller,
                        pageChunksList,
                        recordIndexes,
                        queryState.getNumRecordsInCurPage(),
                        queryArgs.numChunksInRange(),
                        queryArgs.chunkSize(),
                        catalogName);
                blocks[collectParams.getBlockIndex()] = new LazyBlock(rowsToFill, new LazyCollectorLoader(
                        lazyCollectTxService,
                        lazyCollectorLoaderArgs,
                        dictionaryStats,
                        queryArgs.dispatcherPageSourceStats(),
                        shapingLoggerFactory,
                        queryArgs.nativeStats()));
                queryArgs.dispatcherPageSourceStats().inclazy_collect_total_blocks();
            }
        }
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

    public List<Boolean> getPreLoadedBlocks(QueryArgs queryArgs)
    {
        if (!globalConfig.getEnableLazyForSelective()) {
            // in full-scan we always load lazily
            if (queryArgs.queryParams().getNumMatchElements() == 0) {
                return Collections.nCopies(queryArgs.queryParams().getNumCollectElements(), false);
            }
            return Collections.nCopies(queryArgs.queryParams().getNumCollectElements(), true);
        }
        return queryArgs.queryParams().getCollectElementsParamsList().stream()
                .map(WarmupElementCollectParams::hasMatchCollect)
                .toList();
    }

    private AggregatorArgs getStorageCollectorArgs(QueryArgs queryArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();

        ArrayList<BlockFiller<?>> blockFillers = new ArrayList<>(queryParams.getNumCollectElements());
        for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
            blockFillers.add(blockFillersFactory.getBlockFiller(collectParams.getBlockRecTypeCode().ordinal()));
        }

        List<Boolean> preLoadedBlocks = getPreLoadedBlocks(queryArgs);
        CollectBuffersParams collectBuffersParams = collectTxService.getCollectBuffersAllocationParams(queryParams);
        List<WarmupElementCollectParams> preLoadedCollectParamsList = IntStream.range(0, preLoadedBlocks.size())
                .filter(preLoadedBlocks::get)
                .mapToObj(queryParams.getCollectElementsParamsList()::get)
                .toList();
        return new AggregatorArgs(blockFillers,
                collectBuffersParams,
                preLoadedBlocks,
                preLoadedCollectParamsList);
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
