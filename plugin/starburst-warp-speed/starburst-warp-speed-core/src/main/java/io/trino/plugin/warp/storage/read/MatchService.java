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
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
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
import io.trino.spi.TrinoException;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_TX_ALLOCATION_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_MATCH_FAILED;
import static io.trino.plugin.warp.storage.read.BaseCollectTxService.INVALID_TX_ID;

@Singleton
public class MatchService
{
    private static final Logger logger = Logger.get(MatchService.class);
    private final ShapingLogger shapingLogger;
    private static final long MATCH_RESULT_MASK = 0x00000000ffffffffL;

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

    public MatchArgs init(QueryArgs queryArgs, CustomStatsContext customStatsContext)
    {
        QueryParams queryParams = queryArgs.queryParams();
        int[] weMatchTree = queryParams.dumpMatchParams();
        LuceneMatcher[] luceneMatchers = new LuceneMatcher[queryParams.getNumLucene()];

        List<ReadJuffersWarmUpElement> matchJuffersWe = queryParams.getMatchElementsParamsList()
                .stream()
                .map(we -> we.hasLuceneParams() ? new ReadJuffersWarmUpElement(bufferAllocator, false) : new ReadJuffersWarmUpElement())
                .collect(Collectors.toList());

        MatchArgs matchArgs = new MatchArgs(weMatchTree, matchJuffersWe, luceneMatchers);
        createLuceneMatchers(queryArgs, matchArgs, customStatsContext); // this call must be after creating the matchJuffersWE

        return matchArgs;
    }

    private void createLuceneMatchers(QueryArgs queryArgs, MatchArgs matchArgs, CustomStatsContext customStatsContext)
    {
        if (matchArgs.luceneMatchers().length == 0) {
            return;
        }

        LucenePageCacheStats lucenePageCacheStats = (LucenePageCacheStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_LUCENE_PAGE_CACHE_KEY);
        DispatcherPageSourceStats dispatcherPageSourceStats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);

        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryArgs.queryParams().getMatchElementsParamsList()) {
            if (matchParams.hasLuceneParams()) {
                matchArgs.luceneMatchers()[matchParams.getLuceneIx()] = new LuceneMatcher(storageEngine,
                        storageEngineConstants,
                        matchArgs.matchJuffersWe().get(matchIx),
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

    public MatchOpenResult open(QueryArgs queryArgs, MatchArgs matchArgs, CollectOpenResult collectOpenResult)
    {
        int matchTxId = INVALID_TX_ID;
        QueryParams queryParams = queryArgs.queryParams();
        if (queryParams.getNumMatchElements() > 0) {
            Optional<MemorySegment> luceneBitmaps = Optional.empty();
            final int luceneBitmapSizePerWE = storageEngineConstants.getPageSize() * queryArgs.numChunksInRange();
            try {
                SequenceLayout warmUpElementAttsLayout = MemoryLayout.sequenceLayout(queryParams.getNumMatchElements(), WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT);
                MemorySegment warmUpElementAtts = Arena.ofAuto().allocate(warmUpElementAttsLayout.byteSize(), ValueLayout.JAVA_BYTE.byteSize());
                Iterator<WarmupElementMatchParams> matchParamsListItr = queryParams.getMatchElementsParamsList().iterator();
                warmUpElementAtts.elements(WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT)
                        .forEach(warmupElementAtt -> {
                            WarmupElementMatchParams matchParams = matchParamsListItr.next();
                            WarmUpElement.setRecTypeCode(warmupElementAtt, matchParams.getRecTypeCode());
                            WarmUpElement.setRecTypeLength(warmupElementAtt, matchParams.getRecTypeLength());
                            WarmUpElement.setWarmUpType(warmupElementAtt, matchParams.getWarmUpType());
                        });

                if (queryParams.getNumLucene() > 0) {
                    final long alignment = 32; // this is the alignment required for intel optimized bitmap operations
                    final long allocSize = (long) luceneBitmapSizePerWE * (long) queryParams.getNumLucene();
                    luceneBitmaps = Optional.of(collectOpenResult.queryMemoryAllocator().allocate(allocSize, alignment));
                }
                matchTxId = (int) storageEngine.matchOpen(queryParams.getTotalNumRecords(),
                        queryArgs.txArgs().fileCookie(),
                        collectOpenResult.queryMemoryId(),
                        queryArgs.txArgs().collectStoreBuff(),
                        queryArgs.txArgs().matchCollectMetadataAddress()[0],
                        queryParams.getNumMatchElements(),
                        queryArgs.numChunksInRange(),
                        matchArgs.weMatchTree(),
                        warmUpElementAtts.address(),
                        collectOpenResult.matchBmAddr(),
                        luceneBitmaps.isPresent() ? luceneBitmaps.get().address() : 0,
                        queryParams.getMinMatchOffset());
            }
            catch (Exception e) {
                // will throw WARP_TX_ALLOCATION_FAILED cause matchTxId == INVALID_TX_ID
                // this will make sure that collect tx will be closed by the caller
                shapingLogger.warn(e, "matchOpen failed");
            }

            if (matchTxId < 0) {
                throw new TrinoException(WARP_TX_ALLOCATION_FAILED, "failed to allocate tx for match");
            }

            int matchIx = 0;
            for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
                // only for lucene
                matchArgs.matchJuffersWe().get(matchIx).createLuceneBuffers(luceneBitmaps.orElse(null),
                        matchParams.hasLuceneParams() ? luceneBitmapSizePerWE * matchParams.getLuceneIx() : 0);
                matchIx++;
            }
        }

        short[] matchedChunksIndexes = new short[queryArgs.numChunksInRange()];
        int[] matchBitmapResetPoints = new int[queryArgs.numChunksInRange()];
        return new MatchOpenResult(matchTxId, matchedChunksIndexes, matchBitmapResetPoints);
    }

    @SuppressWarnings("Finally")
    @NativeInterrupt
    public boolean match(QueryArgs queryArgs, MatchArgs matchArgs, MatchOpenResult matchOpenResult)
    {
        boolean matchExhausted = chunksQueueService.isChunkRangeCompleted(queryArgs.chunksQueue());
        if (matchExhausted) {
            ChunksQueue chunksQueue = queryArgs.chunksQueue();
            if (matchOpenResult.matchTxId() == INVALID_TX_ID) {
                matchExhausted = chunksQueueService.updateChunkRangeFullScan(chunksQueue, queryArgs.numChunks(), queryArgs.numChunksInRange());
                logger.debug("matchIfNeeded matchExhausted %b after full scan update numChunks %d range %d", matchExhausted, queryArgs.numChunks(), queryArgs.numChunksInRange());
            }
            else {
                long matchResult = 0;
                long numChunks = 0;
                boolean luceneSuccess = true;
                int numMatchedChunks = 0;
                int chunkIndex = chunksQueueService.getChunkIndexForMatch(chunksQueue);
                // we loop until either agg result returnes 0 which  means no more chunks (break under if inside the loop)
                // or if numMatchedChunks returned positive from match call which means at least one chunk has a match
                // in addition, on every call to storage engine we check for error
                try {
                    while (numMatchedChunks == 0) { // no match so far
                        numChunks = storageEngine.matchAgg(matchOpenResult.matchTxId(), chunkIndex);
                        if (numChunks < 0) {
                            break;
                        }
                        matchExhausted = numChunks == 0;
                        if (matchExhausted) {
                            break;
                        }

                        if (queryArgs.queryParams().getNumLucene() > 0) {
                            for (int luceneMatcherIx = 0; luceneMatcherIx < matchArgs.luceneMatchers().length; luceneMatcherIx++) {
                                if (!matchArgs.luceneMatchers()[luceneMatcherIx].match(matchOpenResult.matchTxId(), chunkIndex, (int) numChunks)) {
                                    luceneSuccess = false;
                                    break;
                                }
                            }
                            if (!luceneSuccess) {
                                break;
                            }
                        }

                        matchResult = storageEngine.match(matchOpenResult.matchTxId(), chunkIndex, (int) numChunks, matchOpenResult.matchedChunksIndexes(), matchOpenResult.matchBitmapResetPoints());
                        if (matchResult < 0) {
                            break;
                        }
                        chunkIndex = (int) (matchResult & MATCH_RESULT_MASK);
                        numMatchedChunks = (int) (matchResult >> 32);
                        logger.debug("matchResult %x chunkIndex %d numMatchedChunks %d", matchResult, chunkIndex, numMatchedChunks);
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

                if ((numChunks < 0) || !luceneSuccess || (matchResult < 0)) {
                    throw new TrinoException(WARP_UNRECOVERABLE_MATCH_FAILED,
                            "match failed chunkIndex " + chunkIndex + " numChunks " + numChunks + " lucene " + luceneSuccess + " match " + matchResult);
                }

                if (!matchExhausted) {
                    logger.debug("matchIfNeeded matchStartChunkIndex %d matchEndChunkIndex %d numMatchedChunks %d",
                            chunksQueueService.getChunkIndexForMatch(chunksQueue), chunkIndex, numMatchedChunks);
                    chunksQueueService.updateChunkRangeAfterMatch(chunksQueue, chunkIndex, numMatchedChunks, matchOpenResult.matchedChunksIndexes(), matchOpenResult.matchBitmapResetPoints());
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

    public void abort(MatchOpenResult matchOpenResult, Exception e)
    {
        if (matchOpenResult.matchTxId() != INVALID_TX_ID) {
            // In case of native match exception match tx already closed
            if (!isNativeMatchException(e)) {
                storageEngine.matchClose(matchOpenResult.matchTxId());
            }
        }
    }

    public void close(MatchOpenResult matchOpenResult)
    {
        if (matchOpenResult.matchTxId() != INVALID_TX_ID) {
            storageEngine.matchClose(matchOpenResult.matchTxId());
        }
    }
}
