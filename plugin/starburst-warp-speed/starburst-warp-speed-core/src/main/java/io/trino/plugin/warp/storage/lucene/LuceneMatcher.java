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
package io.trino.plugin.warp.storage.lucene;

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_ALL_OR_NOTHING;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_CFE_FILE_LENGTH;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_CFS_FILE_LENGTH;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_IS_VALID_INDEX;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_NATIVE_COOKIE;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_NUM_OF;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_SEGMENTS_FILE_LENGTH;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_SI_FILE_LENGTH;

@SuppressWarnings("deprecation")
public class LuceneMatcher
{
    private static final Logger logger = Logger.get(LuceneMatcher.class);

    private static final String FILE_PREFIX = "_0";
    private static final int JAVA_RC_STOP_EXECUTION = -2; // predicate got from hack
    private static final int JAVA_NON_HACK_PREDICATE = -3; // predicate got from non-hack
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final DispatcherPageSourceStats statsDispatcherPageSource;
    private final LucenePageCacheStats lucenePageCacheStats;
    private final ReadJuffersWarmUpElement juffersWE;
    private final Optional<Query> query; // when query is empty it means that the predicate is from Domain
    private final int matchWeIx;
    private long[] matchParams;
    private final ShapingLogger shapingLogger;
    private int currChunkInRange;

    public LuceneMatcher(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ReadJuffersWarmUpElement juffersWE,
            LuceneQueryMatchData luceneQueryMatchData,
            int matchWeIx,
            int numChunksInRange,
            LucenePageCacheStats lucenePageCacheStats,
            DispatcherPageSourceStats statsDispatcherPageSource,
            GlobalConfig globalConfig)
    {
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.lucenePageCacheStats = lucenePageCacheStats;
        this.juffersWE = juffersWE;
        this.query = Optional.of(luceneQueryMatchData.getQuery());
        this.matchWeIx = matchWeIx;
        this.matchParams = new long[LUCENE_MATCH_JPARAMS_NUM_OF.ordinal() * numChunksInRange];
        this.statsDispatcherPageSource = statsDispatcherPageSource;
        this.shapingLogger = ShapingLogger.getInstance(logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        logger.debug("lucene matcher for luceneQueryMatchData %s", luceneQueryMatchData);
    }

    public boolean match(int matchTxId, int startChunkIndex, int numChunks)
    {
        if (storageEngine.matchLucenePrepare(matchTxId, matchWeIx, startChunkIndex, numChunks, matchParams) < 0) {
            return false;
        }
        currChunkInRange = 0;
        storageEngine.matchLucene(matchTxId, matchWeIx, startChunkIndex, numChunks); // @TODO do this in java without native
        storageEngine.matchLuceneCompleted(matchTxId, matchWeIx, startChunkIndex, numChunks);
        return true;
    }

    /**
     * This method is called from native as part of the match flow.
     * DO NOT CHANGE THE SIGNATURE
     *
     * @return - 0 for success and JAVA_RC_ERR/JAVA_RC_STOP_EXECUTION for error
     */
    @SuppressWarnings("unused")
    int luceneMatch()
    {
        int baseMatchParams = currChunkInRange * LUCENE_MATCH_JPARAMS_NUM_OF.ordinal();
        long nativeCookie = matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_NATIVE_COOKIE.ordinal()];
        // in case native decided to skip a chunk in the range it will mark the cookie as zero and we need to jump to the next one
        // this code is temporary until we create an index for the entire range in one shot
        while (nativeCookie == 0) {
            currChunkInRange++;
            baseMatchParams += LUCENE_MATCH_JPARAMS_NUM_OF.ordinal();
            nativeCookie = matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_NATIVE_COOKIE.ordinal()];
        }

        if (matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_IS_VALID_INDEX.ordinal()] == 0) {
            currChunkInRange++;
            return 0; // return no match
        }

        if (query.isEmpty()) {
            shapingLogger.error("query is not present returning JAVA_NON_HACK_PREDICATE. nativeCookie %x", nativeCookie);
            currChunkInRange++;
            return JAVA_NON_HACK_PREDICATE;
        }

        long startTime = System.currentTimeMillis();
        int maxDocsToFind = (matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_ALL_OR_NOTHING.ordinal()] != 0) ? 1 : (1 << storageEngineConstants.getChunkSizeShift());

        int[] filesLength = new int[4];
        filesLength[LuceneFileType.SI.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_SI_FILE_LENGTH.ordinal()];
        filesLength[LuceneFileType.CFE.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_CFE_FILE_LENGTH.ordinal()];
        filesLength[LuceneFileType.SEGMENTS.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_SEGMENTS_FILE_LENGTH.ordinal()];
        filesLength[LuceneFileType.CFS.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_CFS_FILE_LENGTH.ordinal()];

        int indexUniqueIdInRowGroup = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID.ordinal()];
        int resultBufferOffset = (int) (currChunkInRange * storageEngineConstants.getPageSize());
        if (logger.isDebugEnabled()) {
            logger.debug("nativeCookie %x indexUniqueIdInRowGroup %d maxDocsToFind %d resultBufferOffset %d filesLength %s",
                    nativeCookie, indexUniqueIdInRowGroup, maxDocsToFind, resultBufferOffset, Arrays.toString(filesLength));
        }

        int result = JAVA_RC_STOP_EXECUTION;
        try {
            WarpInputDirectory warpInputDirectory = new WarpInputDirectory(storageEngine,
                    storageEngineConstants,
                    lucenePageCacheStats,
                    indexUniqueIdInRowGroup,
                    juffersWE,
                    nativeCookie,
                    FILE_PREFIX,
                    filesLength);
            IndexReader reader = DirectoryReader.open(warpInputDirectory);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            ByteBuffer luceneBMResultBuffer = juffersWE.getLuceneBMResultBuffer();
            WarpCollector warpCollector = new WarpCollector(luceneBMResultBuffer, resultBufferOffset, maxDocsToFind);
            indexSearcher.search(query.get(), warpCollector);

            result = warpCollector.getCount();
            logger.debug("nativeCookie %x found %d match results", nativeCookie, result);
        }
        catch (IOException e) {
            shapingLogger.error(e, "I/O error returning JAVA_RC_STOP_EXECUTION. nativeCookie %d maxDocsToFind %d resultBufferOffset %d filesLength %s",
                    nativeCookie, maxDocsToFind, resultBufferOffset, Arrays.toString(filesLength));
        }
        catch (Exception e) {
            shapingLogger.error(e, "general error returning JAVA_RC_STOP_EXECUTION. nativeCookie %d maxDocsToFind %d resultBufferOffset %d filesLength %s",
                    nativeCookie, maxDocsToFind, resultBufferOffset, Arrays.toString(filesLength));
        }
        statsDispatcherPageSource.addlucene_execution_time(System.currentTimeMillis() - startTime);
        currChunkInRange++;
        return result;
    }

    static {
        IndexSearcher.setDefaultQueryCachingPolicy(new QueryCachingPolicy()
        {
            @Override
            public void onUse(Query query)
            {
            }

            @SuppressWarnings("CheckedExceptionNotThrown")
            @Override
            public boolean shouldCache(Query query)
            {
                return false;
            }
        });
        IndexSearcher.setMaxClauseCount(Integer.MAX_VALUE);
    }
}
