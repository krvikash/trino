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
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneMatcher;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.TrinoException;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_TX_ALLOCATION_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_MATCH_FAILED;

@Singleton
public class MatchService
        implements Matcher
{
    private static final Logger logger = Logger.get(MatchService.class);
    private final ShapingLogger shapingLogger;
    private static final long PAGE_BM_ALIGN = 32; // this is the alignment required for intel optimized bitmap operations

    BufferAllocator bufferAllocator;
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final ChunksQueueService chunksQueueService;
    private final GlobalConfig globalConfig;

    @Inject
    MatchService(BufferAllocator bufferAllocator,
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ChunksQueueService chunksQueueService,
            GlobalConfig globalConfig)
    {
        this.bufferAllocator = bufferAllocator;
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.chunksQueueService = chunksQueueService;
        this.globalConfig = globalConfig;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    public MatcherArgs open(QueryArgs queryArgs, CustomStatsContext customStatsContext)
    {
        QueryParams queryParams = queryArgs.queryParams();

        List<ReadJuffersWarmUpElement> matchJuffersWe = queryParams.getMatchElementsParamsList()
                .stream()
                .map(we -> we.hasLuceneParams() ? new ReadJuffersWarmUpElement(bufferAllocator, false) : new ReadJuffersWarmUpElement())
                .collect(Collectors.toList());

        MatcherArgs matcherArgs = new MatcherArgs(matchJuffersWe,
                new LuceneMatcher[queryParams.getNumLucene()],
                queryParams.getArena().allocate(storageEngineConstants.getMatchStatePayload() + MatchState.MATCH_STATE_LAYOUT.byteSize(), PAGE_BM_ALIGN));
        createLuceneMatchers(queryArgs, matcherArgs, customStatsContext); // this call must be after creating the matchJuffersWE
        return matcherArgs;
    }

    private void createLuceneMatchers(QueryArgs queryArgs, MatcherArgs matcherArgs, CustomStatsContext customStatsContext)
    {
        if (matcherArgs.luceneMatchers().length == 0) {
            return;
        }

        LucenePageCacheStats lucenePageCacheStats = (LucenePageCacheStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_LUCENE_PAGE_CACHE_KEY);
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);

        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryArgs.queryParams().getMatchElementsParamsList()) {
            if (matchParams.hasLuceneParams()) {
                matcherArgs.luceneMatchers()[matchParams.getLuceneIx()] = new LuceneMatcher(storageEngine,
                        storageEngineConstants,
                        matcherArgs.matchJuffersWe().get(matchIx),
                        matchParams.getLuceneQueryMatchData(),
                        matchIx,
                        queryArgs.numChunksInRange(),
                        queryArgs.queryParams().getFilePath(),
                        lucenePageCacheStats,
                        dispatcherPageSourceStats,
                        globalConfig);
            }
            matchIx++;
        }
    }

    public MatcherPageArgs openPage(QueryArgs queryArgs, MatcherArgs matcherArgs, AggregatorPageArgs aggregatorPageArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();
        Optional<MatchState> matchState = Optional.empty();
        if (queryParams.getNumMatchElements() > 0) {
            Optional<MemorySegment> luceneBitmaps = Optional.empty();
            final int luceneBitmapSizePerWE = storageEngineConstants.getPageSize() * queryArgs.numChunksInRange();
            try {
                if (queryParams.getNumLucene() > 0) {
                    luceneBitmaps = Optional.of(aggregatorPageArgs.queryMemoryAllocator().allocate(
                            (long) luceneBitmapSizePerWE * (long) queryParams.getNumLucene() + PAGE_BM_ALIGN,
                            PAGE_BM_ALIGN));
                }
                long startTime = System.nanoTime();
                matchState = Optional.of(new MatchState(matcherArgs.matchState(),
                        queryArgs,
                        aggregatorPageArgs,
                        luceneBitmaps,
                        storageEngineConstants.getMatchStatePayload()));
                storageEngine.matchOpen(matchState.get().getMemory());
                queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
            }
            catch (Exception e) {
                // throw WARP_TX_ALLOCATION_FAILED to make sure that collect tx will be closed by the caller
                shapingLogger.error(e, "matchOpen failed");
                throw new TrinoException(WARP_TX_ALLOCATION_FAILED, "storage engine failed to open match");
            }

            int matchIx = 0;
            for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
                // only for lucene
                matcherArgs.matchJuffersWe().get(matchIx).createLuceneBuffers(luceneBitmaps.orElse(null),
                        matchParams.hasLuceneParams() ? luceneBitmapSizePerWE * matchParams.getLuceneIx() : 0);
                matchIx++;
            }
        }

        short[] matchedChunksIndexes = new short[queryArgs.numChunksInRange()];
        int[] matchBitmapResetPoints = new int[queryArgs.numChunksInRange()];
        return new MatcherPageArgs(matchState,
                matchedChunksIndexes,
                matchBitmapResetPoints);
    }

    @SuppressWarnings("Finally")
    @NativeInterrupt
    public boolean match(QueryArgs queryArgs, MatcherArgs matcherArgs, MatcherPageArgs matcherPageArgs)
    {
        boolean matchExhausted = chunksQueueService.isChunkRangeCompleted(queryArgs.chunksQueue());
        if (matchExhausted) {
            ChunksQueue chunksQueue = queryArgs.chunksQueue();
            if (matcherPageArgs.matchState().isEmpty()) {
                matchExhausted = chunksQueueService.updateChunkRangeFullScan(chunksQueue, queryArgs.numChunks(), queryArgs.numChunksInRange());
                logger.debug("matchIfNeeded matchExhausted %b after full scan update numChunks %d range %d", matchExhausted, queryArgs.numChunks(), queryArgs.numChunksInRange());
            }
            else {
                long matchResult = 0;
                int numChunks = 0;
                boolean luceneSuccess = true;
                int numMatchedChunks = 0;
                int chunkIndex = chunksQueueService.getChunkIndexForMatch(chunksQueue);
                MemorySegment matchStateMem = matcherPageArgs.matchState().get().getMemory();
                StopWatch readStopWatch = new StopWatch();
                // we loop until either agg result returnes 0 which  means no more chunks (break under if inside the loop)
                // or if numMatchedChunks returned positive from match call which means at least one chunk has a match
                // in addition, on every call to storage engine we check for error
                try {
                    while (numMatchedChunks == 0) { // no match so far
                        readStopWatch.start();
                        numChunks = storageEngine.matchAgg(matchStateMem, chunkIndex);
                        readStopWatch.stop();
                        if (numChunks < 0) {
                            break;
                        }
                        matchExhausted = numChunks == 0;
                        if (matchExhausted) {
                            break;
                        }

                        if (queryArgs.queryParams().getNumLucene() > 0) {
                            for (int luceneMatcherIx = 0; luceneMatcherIx < matcherArgs.luceneMatchers().length; luceneMatcherIx++) {
                                if (!matcherArgs.luceneMatchers()[luceneMatcherIx].match(matcherPageArgs.matchState().get(), chunkIndex, numChunks, queryArgs.dispatcherPageSourceStats())) {
                                    luceneSuccess = false;
                                    break;
                                }
                            }
                            if (!luceneSuccess) {
                                break;
                            }
                        }

                        readStopWatch.start();
                        matchResult = storageEngine.match(matchStateMem.address(), chunkIndex, numChunks, matcherPageArgs.matchedChunksIndexes(), matcherPageArgs.matchBitmapResetPoints());
                        readStopWatch.stop();
                        if (matchResult < 0) {
                            break;
                        }
                        chunkIndex += numChunks;
                        numMatchedChunks = (int) matchResult;
                    }
                }
                catch (Exception e) {
                    if (e instanceof TrinoException trinoException && ExceptionThrower.isNativeException(trinoException)) {
                        if (trinoException.getErrorCode().getCode() == WARP_NATIVE_UNRECOVERABLE_ERROR.toErrorCode().getCode()) {
                            throw new TrinoException(WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR, "failed to match: " + e.getMessage());
                        }
                        else {
                            throw new TrinoException(WARP_NATIVE_MATCH_ERROR, "failed to match: " + e.getMessage());
                        }
                    }
                    else {
                        throw new TrinoException(WARP_MATCH_FAILED, "failed to match: " + e.getMessage());
                    }
                }
                finally {
                    queryArgs.dispatcherPageSourceStats().addnative_read_time(readStopWatch.getNanoTime());
                }

                if ((numChunks < 0) || !luceneSuccess || (matchResult < 0)) {
                    throw new TrinoException(WARP_UNRECOVERABLE_MATCH_FAILED,
                            "match failed chunkIndex " + chunkIndex + " numChunks " + numChunks + " lucene " + luceneSuccess + " match " + matchResult);
                }

                if (!matchExhausted) {
                    chunksQueueService.updateChunkRangeAfterMatch(chunksQueue, chunkIndex, numMatchedChunks, matcherPageArgs.matchedChunksIndexes(), matcherPageArgs.matchBitmapResetPoints());
                }
            }
        }
        return !matchExhausted;
    }

    private boolean isNativeMatchException(Exception e)
    {
        return e instanceof TrinoException trinoException &&
                (trinoException.getErrorCode().equals(WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR.toErrorCode()) || trinoException.getErrorCode().equals(WARP_NATIVE_MATCH_ERROR.toErrorCode()));
    }

    public void abortPage(QueryArgs queryArgs, MatcherPageArgs matcherPageArgs, Exception e)
    {
        if (matcherPageArgs.matchState().isPresent()) {
            // In case of native match exception match tx already closed
            if (!isNativeMatchException(e)) {
                long startTime = System.nanoTime();
                storageEngine.matchClose(matcherPageArgs.matchState().get().getMemory());
                queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
            }
        }
    }

    public void closePage(QueryArgs queryArgs, MatcherPageArgs matcherPageArgs)
    {
        if (matcherPageArgs.matchState().isPresent()) {
            long startTime = System.nanoTime();

            storageEngine.matchClose(matcherPageArgs.matchState().get().getMemory());
            queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
        }
    }

    public long getOffHeapMemoryUsage(QueryArgs queryArgs, MatcherArgs matcherArgs)
    {
        return queryArgs.queryParams().getWarmUpElementMatchParams().map(m -> m.byteSize()).orElse(0L) +
                queryArgs.queryParams().getMatchNodeAtts().byteSize() +
                matcherArgs.matchState().byteSize();
    }
}
