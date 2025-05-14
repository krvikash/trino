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
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeInterrupt;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneMatcher;
import io.trino.plugin.warp.storage.memory.ThreadArena;
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
import static java.util.Objects.requireNonNull;

@Singleton
public class MatchService
        implements Matcher
{
    private static final Logger logger = Logger.get(MatchService.class);
    private final ShapingLogger shapingLogger;

    private final BufferAllocator bufferAllocator;
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final ShapingLoggerFactory shapingLoggerFactory;
    private final NativeConfig nativeConfig;
    private final RangeFillerService rangeFillerService;

    @Inject
    MatchService(BufferAllocator bufferAllocator,
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            RangeFillerService rangeFillerService,
            ShapingLoggerFactory shapingLoggerFactory,
            NativeConfig nativeConfig)
    {
        this.bufferAllocator = bufferAllocator;
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.rangeFillerService = rangeFillerService;
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.nativeConfig = nativeConfig;
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    public MatcherArgs open(QueryArgs queryArgs, CustomStatsContext customStatsContext)
    {
        QueryParams queryParams = queryArgs.queryParams();

        List<ReadJuffersWarmUpElement> matchJuffersWe = queryParams.getMatchElementsParamsList()
                .stream()
                .map(we -> we.hasLuceneParams() ? new ReadJuffersWarmUpElement(bufferAllocator, false) : new ReadJuffersWarmUpElement())
                .collect(Collectors.toList());

        // The Record Indexes buffer should fit to a full page indexes plus the last chunk that might not fit the in the
        // page, while the storeRowListBuff should only fit to the last chunk indexes therefor a chunk size is enough here.
        short[] storeRowListBuff = new short[queryArgs.chunkSize()];
        ChunksQueue chunksQueue = new ChunksQueue(queryArgs.maxMatchedChunks(), queryArgs.chunkSize(), storageEngineConstants.getPageSize());

        MatcherArgs matcherArgs = new MatcherArgs(matchJuffersWe, new LuceneMatcher[queryParams.getNumLucene()], storeRowListBuff, chunksQueue);
        createLuceneMatchers(queryArgs, matcherArgs, customStatsContext); // this call must be after creating the matchJuffersWE
        return matcherArgs;
    }

    private void createLuceneMatchers(QueryArgs queryArgs, MatcherArgs matcherArgs, CustomStatsContext customStatsContext)
    {
        if (matcherArgs.luceneMatchers().length == 0) {
            return;
        }

        LucenePageCacheStats lucenePageCacheStats = (LucenePageCacheStats) customStatsContext.getStat(LucenePageCacheStats.createKey());
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());

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
                        shapingLoggerFactory);
            }
            matchIx++;
        }
    }

    @NativeInterrupt
    public MatcherPageArgs openPage(RecordIndexes recordIndexes,
            ThreadArena pageArena,
            QueryArgs queryArgs,
            MatcherArgs matcherArgs,
            AggregatorPageArgs aggregatorPageArgs)
    {
        QueryParams queryParams = queryArgs.queryParams();
        matcherArgs.chunksQueue().initRootBitmaps();

        Optional<MatchState> matchStateOpt = Optional.empty();
        if (queryParams.getNumMatchElements() > 0) {
            try {
                long startTime = System.nanoTime();
                MatchState matchState = new MatchState(queryArgs,
                        pageArena,
                        aggregatorPageArgs.matchCollectMetadata(),
                        storageEngineConstants.getMatchStatePayload(),
                        nativeConfig.getLimitNumIosInParallel() * nativeConfig.getMaxIOMetadataSize(),
                        storageEngineConstants.getPageSize());
                storageEngine.matchOpen(matchState.getStateMemory());
                matchStateOpt = Optional.of(matchState);
                queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
            }
            catch (Throwable t) {
                shapingLogger.error(t, "matchOpen failed");
                if (t instanceof TrinoException trinoException && ExceptionThrower.isNativeException(trinoException)) {
                    throw new TrinoException(WARP_NATIVE_MATCH_ERROR, "failed to open match " + t.getMessage(), t);
                }
                else {
                    throw new TrinoException(WARP_MATCH_ERROR, "failed to open match " + t.getMessage(), t);
                }
            }

            final int luceneBitmapSizePerWE = storageEngineConstants.getPageSize() * queryArgs.numChunksInRange();
            int matchIx = 0;
            for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
                // only for lucene
                matcherArgs.matchJuffersWe().get(matchIx).createLuceneBuffers(matchStateOpt.map(m -> m.getLuceneBitmaps().orElse(null)).orElse(null),
                        matchParams.hasLuceneParams() ? luceneBitmapSizePerWE * matchParams.getLuceneIx() : 0);
                matchIx++;
            }
        }
        matchStateOpt.ifPresent(m -> matcherArgs.chunksQueue().setRootBitmaps(m.getMatchBitmaps(), m.getRootBitmapsDescriptors()));

        // restore processed chunk
        matcherArgs.chunksQueue().getOptLoadedChunkProperties().ifPresent(chunk ->
                recordIndexes.restoreRowList(chunk, matcherArgs.storeRowListBuff()));

        RangeData rangeData = new RangeData();
        return new MatcherPageArgs(matchStateOpt, recordIndexes, rangeData);
    }

    @SuppressWarnings("Finally")
    @NativeInterrupt
    private boolean matchMultiChunk(ChunksQueue chunksQueue, QueryArgs queryArgs, MatcherArgs matcherArgs, MatcherPageArgs matcherPageArgs)
    {
        boolean matchExhausted = chunksQueue.matchedChunksCompleted();
        if (matchExhausted) {
            if (matcherPageArgs.matchState().isEmpty()) {
                matchExhausted = chunksQueue.updateChunkRangeFullScan(queryArgs.numChunks(), queryArgs.numChunksInRange());
                logger.debug("matchIfNeeded matchExhausted %b after full scan update numChunks %d range %d", matchExhausted, queryArgs.numChunks(), queryArgs.numChunksInRange());
            }
            else {
                int numChunks = 0;
                boolean matchSuccess = true;
                boolean luceneSuccess = true;
                boolean hasMacthedChunks = false;
                MemorySegment matchStateMem = matcherPageArgs.matchState().get().getStateMemory();
                StopWatch readStopWatch = new StopWatch();
                // we loop until either agg result returns 0 which means no more chunks (break under if inside the loop)
                // or if chunkQueue indicates after match call that at least one chunk has matched records
                // in addition, on every call to storage engine we check for error
                try {
                    while (!hasMacthedChunks) { // no match so far
                        readStopWatch.start();
                        numChunks = storageEngine.matchAgg(matchStateMem, chunksQueue.getChunkIndexForMatch());
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
                                if (!matcherArgs.luceneMatchers()[luceneMatcherIx].match(matcherPageArgs.matchState().get(), chunksQueue.getChunkIndexForMatch(), numChunks, queryArgs.dispatcherPageSourceStats())) {
                                    luceneSuccess = false;
                                    break;
                                }
                            }
                            if (!luceneSuccess) {
                                break;
                            }
                        }

                        readStopWatch.start();
                        matchSuccess = storageEngine.match(matchStateMem, chunksQueue.getChunkIndexForMatch(), numChunks);
                        readStopWatch.stop();
                        if (!matchSuccess) {
                            break;
                        }
                        hasMacthedChunks = chunksQueue.anyMatchedChunkExists(numChunks);
                    }
                }
                catch (Exception e) {
                    if (e instanceof TrinoException trinoException && ExceptionThrower.isNativeException(trinoException)) {
                        if (trinoException.getErrorCode().getCode() == WARP_NATIVE_UNRECOVERABLE_ERROR.toErrorCode().getCode()) {
                            throw new TrinoException(WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR, "failed to match: " + e.getMessage(), e);
                        }
                        else {
                            throw new TrinoException(WARP_NATIVE_MATCH_ERROR, "failed to match: " + e.getMessage(), e);
                        }
                    }
                    else {
                        throw new TrinoException(WARP_MATCH_ERROR, "failed to match: " + e.getMessage(), e);
                    }
                }
                finally {
                    queryArgs.dispatcherPageSourceStats().addnative_read_time(readStopWatch.getNanoTime());
                }

                if ((numChunks < 0) || !luceneSuccess || !matchSuccess) {
                    throw new TrinoException(WARP_UNRECOVERABLE_MATCH_FAILED,
                            "match failed numChunks " + numChunks + " lucene " + luceneSuccess + " match " + matchSuccess);
                }

                if (!matchExhausted) {
                    chunksQueue.updateChunkRangeAfterMatch(numChunks);
                }
            }
        }
        return !matchExhausted;
    }

    public Optional<ChunkProperties> match(int recordsPageLimit,
            QueryArgs queryArgs,
            MatcherArgs matcherArgs,
            MatcherPageArgs matcherPageArgs,
            WarpQueryState queryState)
    {
        ChunksQueue chunksQueue = matcherArgs.chunksQueue();
        RecordIndexes recordIndexes = matcherPageArgs.recordIndexes();
        int numRecordsInPage = queryState.getNumRecordsInCurPage();

        // done with this page
        // in full-scan we dont return more than 1 chunk in a page
        if (numRecordsInPage >= recordsPageLimit ||
                (queryArgs.queryParams().getNumMatchElements() == 0 && numRecordsInPage > 0)) {
            return Optional.empty();
        }

        // if done with matched chunks - match. If finished to match all the chunks in the split return empty chunk.
        if (chunksQueue.matchedChunksCompleted() && !matchMultiChunk(chunksQueue, queryArgs, matcherArgs, matcherPageArgs)) {
            return Optional.empty();
        }

        // prepare stored/matched bms.
        ChunkProperties chunk = chunksQueue.loadChunk(recordIndexes, queryArgs.numRecordsInChunk(chunksQueue.getCurrent()));

        if (chunk.numRecordsInChunk() + numRecordsInPage > recordsPageLimit) {
            // we prefer to avoid decompressing twice the same chunk so if we can we wait for the next page and collect the entire chunk
            // return this chunk in the next page
            if (numRecordsInPage > 0) {
                chunksQueue.returnLoadedChunk(chunk);
                return Optional.empty();
            }
            // return a partial chunk in this page and store the rest
            int extraInLastChunk = chunk.numRecordsInChunk() + numRecordsInPage - recordsPageLimit;
            chunk.reduceNumRecordsInChunk(extraInLastChunk);
            ChunkProperties chunkToStore = new ChunkProperties(chunk.chunkIndex(),
                    extraInLastChunk,
                    chunk.type(),
                    chunk.startIx() + chunk.numRecordsInChunk());
            chunksQueue.returnLoadedChunk(chunkToStore);
        }

        int numToCollect = rangeFillerService.add(recordIndexes, chunk, queryArgs, matcherPageArgs.rangeData());
        queryState.addToNumRecordsInCurPage(numToCollect);

        return Optional.of(chunk);
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

    @Override
    public WarpStoragePageSource.RowRanges getRanges(MatcherPageArgs matcherPageArgs)
    {
        return rangeFillerService.collectRanges(matcherPageArgs.rangeData());
    }

    @NativeInterrupt
    public void closePage(QueryArgs queryArgs, MatcherArgs matcherArgs, MatcherPageArgs matcherPageArgs)
    {
        // store records list if needed
        matcherArgs.chunksQueue().getOptLoadedChunkProperties().ifPresent(chunk ->
                matcherPageArgs.recordIndexes().storeRowList(chunk, matcherArgs.storeRowListBuff()));

        // store the bitmaps that were added during this round and not processed
        matcherArgs.chunksQueue().storeMatchBitmaps(queryArgs);
        if (matcherPageArgs.matchState().isPresent()) {
            long startTime = System.nanoTime();

            storageEngine.matchClose(matcherPageArgs.matchState().get().getStateMemory());
            queryArgs.dispatcherPageSourceStats().addnative_read_time(System.nanoTime() - startTime);
        }
    }
}
