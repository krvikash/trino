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
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.storage.read.MatchState;
import io.trino.spi.TrinoException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ThreadInterruptedException;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;

import static io.trino.plugin.warp.WarpErrorCode.WARP_MATCH_LUCENE_FAILED;

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
    private final String rowGroupFilePath;
    private final int matchOffset;
    private final ShapingLogger shapingLogger;
    private int currChunkInRange;

    public LuceneMatcher(
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ReadJuffersWarmUpElement juffersWE,
            LuceneQueryMatchData luceneQueryMatchData,
            int matchWeIx,
            int numChunksInRange,
            String rowGroupFilePath,
            LucenePageCacheStats lucenePageCacheStats,
            DispatcherPageSourceStats statsDispatcherPageSource,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.lucenePageCacheStats = lucenePageCacheStats;
        this.juffersWE = juffersWE;
        this.query = luceneQueryMatchData.getQuery();
        this.matchWeIx = matchWeIx;
        this.rowGroupFilePath = rowGroupFilePath;
        this.matchOffset = luceneQueryMatchData.getWarmUpElement().getMatchOffset();
        this.statsDispatcherPageSource = statsDispatcherPageSource;
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
        logger.debug("lucene matcher for luceneQueryMatchData %s", luceneQueryMatchData);
    }

    public boolean match(MatchState matchState, int startChunkIndex, int numChunks, DispatcherPageSourceStats dispatcherPageSourceStats)
            throws InterruptedException
    {
        ChunkStateHandler chunkStateHandler = new ChunkStateHandler(storageEngineConstants, rowGroupFilePath, matchOffset);
        List<ChunkState> chunkStates = null;
        int loadedPageIndex = -1;
        MemorySegment luceneState = matchState.getMatchLuceneState();

        logger.debug(
                "match rowGroupFilePath %s matchOffset %d startChunkIndex %d numChunks %d",
                rowGroupFilePath,
                matchOffset,
                startChunkIndex,
                numChunks);

        // loop to perform the match chunk by chunk
        currChunkInRange = 0;
        while (currChunkInRange < numChunks) {
            int chunkIndex = startChunkIndex + currChunkInRange;
            int pageIndex = chunkStateHandler.getPageIndex(chunkIndex);

            if (pageIndex != loadedPageIndex) {
                chunkStates = chunkStateHandler.load(pageIndex);
                loadedPageIndex = pageIndex;
            }

            long startTime = System.nanoTime();
            boolean success = storageEngine.matchLucenePrepare(matchState.getStateMemory(), matchWeIx, chunkIndex);
            dispatcherPageSourceStats.addnative_read_time(System.nanoTime() - startTime);
            if (!success) {
                return false;
            }

            int indexUniqueIdInRowGroup = matchState.getLuceneUniqueId(luceneState);
            if (indexUniqueIdInRowGroup >= 0) {
                int chunkIndexInPage = chunkStateHandler.getChunkIndexInPage(startChunkIndex + currChunkInRange);
                int numMatchedRecords = luceneMatch(chunkStates.get(chunkIndexInPage), indexUniqueIdInRowGroup, matchState.getLuceneNumRecords(luceneState));
                storageEngine.matchLuceneCompleted(matchState.getStateMemory(), matchWeIx, chunkIndex, numMatchedRecords);
            }
            currChunkInRange++;
        }
        return true;
    }

    /**
     * do the actual match and return number of matched records
     */
    int luceneMatch(ChunkState chunkState, int indexUniqueIdInRowGroup, int docsToFind)
            throws InterruptedException
    {
        long startTime = System.currentTimeMillis();
        int resultBufferOffset = (int) (currChunkInRange * storageEngineConstants.getPageSize());
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "luceneMatch currChunkInRange %d indexUniqueIdInRowGroup %d docsToFind %d resultBufferOffset %d chunkState %s",
                    currChunkInRange,
                    indexUniqueIdInRowGroup,
                    docsToFind,
                    resultBufferOffset,
                    chunkState);
        }

        try {
            LuceneIndexReader luceneIndexReader = new LuceneIndexReader(storageEngineConstants, rowGroupFilePath, chunkState);

            WarpInputDirectory warpInputDirectory = new WarpInputDirectory(
                    luceneIndexReader,
                    storageEngineConstants,
                    lucenePageCacheStats,
                    indexUniqueIdInRowGroup,
                    FILE_PREFIX,
                    chunkState.filesLength());
            IndexReader reader = DirectoryReader.open(warpInputDirectory);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            ByteBuffer luceneBMResultBuffer = juffersWE.getLuceneBMResultBuffer();
            WarpCollector warpCollector = new WarpCollector(luceneBMResultBuffer, resultBufferOffset, docsToFind);
            indexSearcher.search(query, warpCollector);
            return warpCollector.getCount();
        }
        catch (ThreadInterruptedException threadInterruptedException) {
            throw (InterruptedException) threadInterruptedException.getCause();
        }
        catch (Exception e) {
            shapingLogger.error(
                    e,
                    "error in lucene search indexUniqueIdInRowGroup %d docsToFind %d resultBufferOffset %d chunkState %s",
                    indexUniqueIdInRowGroup,
                    docsToFind,
                    resultBufferOffset,
                    chunkState);
            throw new TrinoException(WARP_MATCH_LUCENE_FAILED, e.getMessage(), e);
        }
        finally {
            statsDispatcherPageSource.addlucene_execution_time(System.currentTimeMillis() - startTime);
        }
    }
}
