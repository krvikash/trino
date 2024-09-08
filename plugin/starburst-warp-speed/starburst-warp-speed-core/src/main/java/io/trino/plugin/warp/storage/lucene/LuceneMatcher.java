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
import java.util.List;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_LUCENE_FAILED;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_DOCS_TO_FIND;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID;
import static io.trino.plugin.warp.gen.constants.LuceneMatchJParams.LUCENE_MATCH_JPARAMS_NUM_OF;

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
    private final long[] matchParams;
    private final int[] matchResult;
    private final String rowGroupFilePath;
    private final int matchOffset;
    private final ShapingLogger shapingLogger;
    private int currChunkInRange;

    public LuceneMatcher(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ReadJuffersWarmUpElement juffersWE,
            LuceneQueryMatchData luceneQueryMatchData,
            int matchWeIx,
            int numChunksInRange,
            String rowGroupFilePath,
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
        this.rowGroupFilePath = rowGroupFilePath;
        this.matchOffset = luceneQueryMatchData.getWarmUpElement().getMatchOffset();
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

        ChunkStateHandler chunkStateHandler = new ChunkStateHandler(storageEngineConstants, rowGroupFilePath, matchOffset);
        List<ChunkState> chunkStates = null;
        int loadedPageIndex = -1;

        logger.debug("match rowGroupFilePath %s matchOffset %d matchTxId %d startChunkIndex %d numChunks %d",
                rowGroupFilePath, matchOffset, matchTxId, startChunkIndex, numChunks);

        // loop to perform the match chunk by chunk
        currChunkInRange = 0;
        while (currChunkInRange < numChunks) {
            int pageIndex = chunkStateHandler.getPageIndex(startChunkIndex + currChunkInRange);

            if (pageIndex != loadedPageIndex) {
                chunkStates = chunkStateHandler.load(pageIndex);
                loadedPageIndex = pageIndex;
            }

            int chunkIndex = chunkStateHandler.getChunkIndexInPage(startChunkIndex + currChunkInRange);

            luceneMatch(chunkStates.get(chunkIndex));
            currChunkInRange++;
        }

        // pass storage engine the match result and free resources
        storageEngine.matchLuceneCompleted(matchTxId, matchWeIx, startChunkIndex, numChunks, matchResult);
        return true;
    }

    /**
     * do the actual match
     */
    void luceneMatch(ChunkState chunkState)
    {
        int baseMatchParams = currChunkInRange * LUCENE_MATCH_JPARAMS_NUM_OF.ordinal();
        long indexUniqueIdInRowGroup = matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID.ordinal()];
        // in case native decided to skip a chunk in the range it will mark the cookie as zero and we need to skip thie one
        // another case for skip is if index is invalid
        if (indexUniqueIdInRowGroup == -1) {
            matchResult[currChunkInRange] = 0; // return no match
            return;
        }

        long startTime = System.currentTimeMillis();
        int docsToFind = (int) matchParams[baseMatchParams + LUCENE_MATCH_JPARAMS_DOCS_TO_FIND.ordinal()];
        int resultBufferOffset = (int) (currChunkInRange * storageEngineConstants.getPageSize());

        if (logger.isDebugEnabled()) {
            logger.debug("luceneMatch currChunkInRange %d indexUniqueIdInRowGroup %d docsToFind %d resultBufferOffset %d chunkState %s",
                    currChunkInRange, indexUniqueIdInRowGroup, docsToFind, resultBufferOffset, chunkState);
        }

        try {
            LuceneIndexReader luceneIndexReader = new LuceneIndexReader(storageEngineConstants, rowGroupFilePath, chunkState);

            WarpInputDirectory warpInputDirectory = new WarpInputDirectory(luceneIndexReader,
                    storageEngineConstants,
                    lucenePageCacheStats,
                    (int) indexUniqueIdInRowGroup,
                    FILE_PREFIX,
                    chunkState.filesLength());
            IndexReader reader = DirectoryReader.open(warpInputDirectory);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            ByteBuffer luceneBMResultBuffer = juffersWE.getLuceneBMResultBuffer();
            WarpCollector warpCollector = new WarpCollector(luceneBMResultBuffer, resultBufferOffset, docsToFind);
            indexSearcher.search(query, warpCollector);
            matchResult[currChunkInRange] = warpCollector.getCount();
        }
        catch (Exception e) {
            shapingLogger.error(e, "error in lucene search indexUniqueIdInRowGroup %d docsToFind %d resultBufferOffset %d chunkState %s",
                    indexUniqueIdInRowGroup, docsToFind, resultBufferOffset, chunkState);
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
