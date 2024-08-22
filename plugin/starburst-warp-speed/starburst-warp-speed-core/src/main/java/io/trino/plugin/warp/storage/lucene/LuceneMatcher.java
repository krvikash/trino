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
import io.trino.spi.TrinoException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_LUCENE_FAILED;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_CFE_FILE_LENGTH;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_CFS_FILE_LENGTH;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_DOCS_TO_FIND;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_NATIVE_COOKIE;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_NUM_OF;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_SEGMENTS_FILE_LENGTH;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_SI_FILE_LENGTH;

@SuppressWarnings("deprecation")
public class LuceneMatcher
{
    private static final Logger logger = Logger.get(LuceneMatcher.class);

    private static final String FILE_PREFIX = "_0";
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final DispatcherPageSourceStats statsDispatcherPageSource;
    private final LucenePageCacheStats lucenePageCacheStats;
    private final ReadJuffersWarmUpElement juffersWE;
    private final Query query;
    private final int matchWeIx;
    private long[] matchParams;
    private int[] matchResult;
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
        this.query = luceneQueryMatchData.getQuery();
        this.matchWeIx = matchWeIx;
        this.matchParams = new long[LUCENE_MATCH_JPARAMS_NUM_OF.ordinal() * numChunksInRange];
        this.matchResult = new int[numChunksInRange];
        this.statsDispatcherPageSource = statsDispatcherPageSource;
        this.shapingLogger = ShapingLogger.getInstance(logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
        logger.debug("lucene matcher for luceneQueryMatchData %s", luceneQueryMatchData);
    }

    public boolean match(int matchTxId, int startChunkIndex, int numChunks)
    {
        // call storage engine to prepare the match and get the parameters
        if (storageEngine.matchLucenePrepare(matchTxId, matchWeIx, startChunkIndex, numChunks, matchParams) < 0) {
            return false;
        }

        // loop to perform the match chunk by chunk
        currChunkInRange = 0;
        while (currChunkInRange < numChunks) {
            luceneMatch(matchTxId);
            currChunkInRange++;
        }

        // pass storage engine the match result and free resources
        storageEngine.matchLuceneCompleted(matchTxId, matchWeIx, startChunkIndex, numChunks, matchResult);
        return true;
    }

    /**
     * do the actual match
     */
    void luceneMatch(int matchTxId)
    {
        int baseMatchParams = currChunkInRange * LUCENE_MATCH_JPARAMS_NUM_OF.ordinal();
        long nativeCookie = matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_NATIVE_COOKIE.ordinal()];

        // in case native decided to skip a chunk in the range it will mark the cookie as zero and we need to skip thie one
        // another case for skip is if index is invalid
        if (nativeCookie == 0) {
            matchResult[currChunkInRange] = 0; // return no match
            return;
        }

        long startTime = System.currentTimeMillis();
        int docsToFind = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_DOCS_TO_FIND.ordinal()];

        int[] filesLength = new int[4];
        filesLength[LuceneFileType.SI.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_SI_FILE_LENGTH.ordinal()];
        filesLength[LuceneFileType.CFE.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_CFE_FILE_LENGTH.ordinal()];
        filesLength[LuceneFileType.SEGMENTS.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_SEGMENTS_FILE_LENGTH.ordinal()];
        filesLength[LuceneFileType.CFS.getNativeId()] = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_CFS_FILE_LENGTH.ordinal()];

        int indexUniqueIdInRowGroup = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID.ordinal()];
        int resultBufferOffset = (int) (currChunkInRange * storageEngineConstants.getPageSize());
        if (logger.isDebugEnabled()) {
            logger.debug("currChunkInRange %d nativeCookie %x indexUniqueIdInRowGroup %d docsToFind %d resultBufferOffset %d filesLength %s",
                    currChunkInRange, nativeCookie, indexUniqueIdInRowGroup, docsToFind, resultBufferOffset, Arrays.toString(filesLength));
        }

        try {
            WarpInputDirectory warpInputDirectory = new WarpInputDirectory(storageEngine,
                    storageEngineConstants,
                    lucenePageCacheStats,
                    indexUniqueIdInRowGroup,
                    juffersWE,
                    nativeCookie,
                    matchTxId,
                    FILE_PREFIX,
                    filesLength);
            IndexReader reader = DirectoryReader.open(warpInputDirectory);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            ByteBuffer luceneBMResultBuffer = juffersWE.getLuceneBMResultBuffer();
            WarpCollector warpCollector = new WarpCollector(luceneBMResultBuffer, resultBufferOffset, docsToFind);
            indexSearcher.search(query, warpCollector);
            matchResult[currChunkInRange] = warpCollector.getCount();
        }
        catch (Exception e) {
            shapingLogger.error(e, "error in lucene search nativeCookie %d docsToFind %d resultBufferOffset %d filesLength %s",
                    nativeCookie, docsToFind, resultBufferOffset, Arrays.toString(filesLength));
            throw new TrinoException(WARP_MATCH_LUCENE_FAILED, e.getMessage());
        }
        finally {
            statsDispatcherPageSource.addlucene_execution_time(System.currentTimeMillis() - startTime);
        }
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
