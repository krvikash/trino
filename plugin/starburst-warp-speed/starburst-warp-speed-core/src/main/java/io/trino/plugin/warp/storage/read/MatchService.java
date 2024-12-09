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

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_MATCH_FAILED;

@Singleton
public class MatchService
        implements Matcher
{
    private static final Logger logger = Logger.get(MatchService.class);
    private final ShapingLogger shapingLogger;

    BufferAllocator bufferAllocator;
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final GlobalConfig globalConfig;

    @Inject
    MatchService(BufferAllocator bufferAllocator,
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            GlobalConfig globalConfig)
    {
        this.bufferAllocator = bufferAllocator;
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
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

        // +1 below is for the current bitmaps set that is used as an intermediate bitmap by native layer
        final int matchTreeHeight = queryParams.getRootMatchNode().map(r -> r.getHeight() + 1).orElse(0);
        MatcherArgs matcherArgs = new MatcherArgs(matchJuffersWe,
                new LuceneMatcher[queryParams.getNumLucene()],
                new MatchState(storageEngineConstants.getMatchStatePayload(),
                        storageEngineConstants.getPageSize(),
                        queryArgs.numChunksInRange() * matchTreeHeight,
                        queryArgs.numChunksInRange() * queryParams.getNumLucene()));
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

    public MatcherPageArgs openPage(ChunksQueue chunksQueue, QueryArgs queryArgs, MatcherArgs matcherArgs, AggregatorPageArgs aggregatorPageArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();
        chunksQueue.initRootBitmaps();

        Optional<MatchState> matchStateOpt = Optional.empty();
        if (queryParams.getNumMatchElements() > 0) {
            MatchState matchState = matcherArgs.matchState();
            try {
                long startTime = System.nanoTime();
                // at this point we still keep setMemory and setState as separate APIs allthough they are called one after the other
                // we can revisit this when we are done refactoring the memory usage
                matchState.setMemory(queryArgs);
                matchState.setState(queryArgs, aggregatorPageArgs.queryMemoryId());
                storageEngine.matchOpen(matchState.getStateMemory());
                matchStateOpt = Optional.of(matchState);
                queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
            }
            catch (Throwable t) {
                shapingLogger.error(t, "matchOpen failed");
                if (t instanceof TrinoException trinoException && ExceptionThrower.isNativeException(trinoException)) {
                    throw new TrinoException(WARP_NATIVE_MATCH_ERROR, "failed to open match " + t.getMessage());
                }
                else {
                    throw new TrinoException(WARP_MATCH_ERROR, "failed to open match " + t.getMessage());
                }
            }

            final int luceneBitmapSizePerWE = storageEngineConstants.getPageSize() * queryArgs.numChunksInRange();
            int matchIx = 0;
            for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
                // only for lucene
                matcherArgs.matchJuffersWe().get(matchIx).createLuceneBuffers(matchState.getLuceneBitmaps().orElse(null),
                        matchParams.hasLuceneParams() ? luceneBitmapSizePerWE * matchParams.getLuceneIx() : 0);
                matchIx++;
            }
        }
        matchStateOpt.ifPresent(m -> chunksQueue.setRootBitmaps(m.getMatchBitmaps(), m.getRootBitmapsDescriptors()));

        return new MatcherPageArgs(matchStateOpt, new short[queryArgs.numChunksInRange()]);
    }

    @SuppressWarnings("Finally")
    @NativeInterrupt
    public boolean match(ChunksQueue chunksQueue, QueryArgs queryArgs, MatcherArgs matcherArgs, MatcherPageArgs matcherPageArgs)
    {
        boolean matchExhausted = chunksQueue.isChunkRangeCompleted();
        if (matchExhausted) {
            if (matcherPageArgs.matchState().isEmpty()) {
                matchExhausted = chunksQueue.updateChunkRangeFullScan(queryArgs.numChunks(), queryArgs.numChunksInRange());
                logger.debug("matchIfNeeded matchExhausted %b after full scan update numChunks %d range %d", matchExhausted, queryArgs.numChunks(), queryArgs.numChunksInRange());
            }
            else {
                long matchResult = 0;
                int numChunks = 0;
                boolean luceneSuccess = true;
                int numMatchedChunks = 0;
                int chunkIndex = chunksQueue.getChunkIndexForMatch();
                MemorySegment matchStateMem = matcherPageArgs.matchState().get().getStateMemory();
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
                        matchResult = storageEngine.match(matchStateMem.address(), chunkIndex, numChunks, matcherPageArgs.matchedChunksIndexes());
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
                        throw new TrinoException(WARP_MATCH_ERROR, "failed to match: " + e.getMessage());
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
                    chunksQueue.updateChunkRangeAfterMatch(chunkIndex, numMatchedChunks, matcherPageArgs.matchedChunksIndexes());
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
                storageEngine.matchClose(matcherPageArgs.matchState().get().getStateMemory());
                queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
            }
        }
    }

    public void closePage(QueryArgs queryArgs, MatcherPageArgs matcherPageArgs)
    {
        if (matcherPageArgs.matchState().isPresent()) {
            long startTime = System.nanoTime();

            storageEngine.matchClose(matcherPageArgs.matchState().get().getStateMemory());
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
