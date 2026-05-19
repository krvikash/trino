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
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class WarpInputDirectory
        extends BaseDirectory
{
    private static final Logger logger = Logger.get(WarpInputDirectory.class);

    private static final int MAX_PAGE_CACHE_ENTRIES = 128;

    private final ByteBuffer[] smallFilePageCache = new ByteBuffer[LuceneFileType.numSmallFiles()];
    private final Map<LucenePageCacheKey, ByteBuffer> bigFilePageCache = new LinkedHashMap<>()
    {
        @Override
        protected boolean removeEldestEntry(Map.Entry<LucenePageCacheKey, ByteBuffer> eldest)
        {
            return size() > MAX_PAGE_CACHE_ENTRIES;
        }
    };

    private final LuceneIndexReader luceneIndexReader;
    private final StorageEngineConstants storageEngineConstants;
    private final LucenePageCacheStats lucenePageCacheStats;
    private final int indexUniqueIdInRowGroup;
    private final int[] fileLengths;
    private final String filePrefix;

    protected WarpInputDirectory(
            LuceneIndexReader luceneIndexReader,
            StorageEngineConstants storageEngineConstants,
            LucenePageCacheStats lucenePageCacheStats,
            int indexUniqueIdInRowGroup,
            String filePrefix,
            int[] fileLengths)
    {
        super(NoLockFactory.INSTANCE);
        this.luceneIndexReader = luceneIndexReader;
        this.storageEngineConstants = storageEngineConstants;
        this.lucenePageCacheStats = lucenePageCacheStats;
        this.indexUniqueIdInRowGroup = indexUniqueIdInRowGroup;
        this.filePrefix = filePrefix;
        this.fileLengths = fileLengths;
    }

    @Override
    public String[] listAll()
    {
        // need to combine temporary with constant files??
        // is this api used only in read?
        // is this api used to get the list of files before the merge begin

        String[] fileNames = new String[LuceneFileType.values().length - 1];
        fileNames[LuceneFileType.CFE.getFileId()] = filePrefix + ".cfe";
        fileNames[LuceneFileType.CFS.getFileId()] = filePrefix + ".cfs";
        fileNames[LuceneFileType.SI.getFileId()] = filePrefix + ".si";
        fileNames[LuceneFileType.SEGMENTS.getFileId()] = "segments_1";
        logger.debug("indexUniqueIdInRowGroup=%d, listAll %s", indexUniqueIdInRowGroup, Arrays.toString(fileNames));
        return fileNames;
    }

    @Override
    public void deleteFile(String fileName)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, delete file %s while input", indexUniqueIdInRowGroup, fileName);
    }

    @Override
    public long fileLength(String fileName)
    {
        LuceneFileType type = LuceneFileType.getType(fileName);
        long len = fileLengths[type.getFileId()];
        logger.debug("indexUniqueIdInRowGroup=%d, fileLength = %d", indexUniqueIdInRowGroup, len);
        return len;
    }

    @Override
    public IndexOutput createOutput(String fileName, IOContext ioContext)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, createTempOutput file %s while input", indexUniqueIdInRowGroup, fileName);
        return null;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, createTempOutput file %s while input", indexUniqueIdInRowGroup, prefix);
        return null;
    }

    @Override
    public void sync(Collection<String> collection)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, sync file while input", indexUniqueIdInRowGroup);
    }

    @Override
    public void syncMetaData()
    {
        logger.debug("indexUniqueIdInRowGroup=%d, syncMetadata file while input", indexUniqueIdInRowGroup);
    }

    @Override
    public void rename(String source, String dest)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, rename file %s while input", indexUniqueIdInRowGroup, source);
    }

    @Override
    public IndexInput openInput(String fileName, IOContext ioContext)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, openInput file %s while input", indexUniqueIdInRowGroup, fileName);
        LuceneFileType luceneFileType = LuceneFileType.getType(fileName);
        return new WarpReadIndexInput(
                luceneIndexReader,
                storageEngineConstants,
                lucenePageCacheStats,
                smallFilePageCache,
                bigFilePageCache,
                indexUniqueIdInRowGroup,
                luceneFileType,
                0,
                fileLengths[luceneFileType.getFileId()],
                "root");
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String fileName)
    {
        logger.debug("indexUniqueIdInRowGroup=%d, openChecksumInput file %s while input", indexUniqueIdInRowGroup, fileName);
        LuceneFileType luceneFileType = LuceneFileType.getType(fileName);
        return new WarpReadIndexInput(
                luceneIndexReader,
                storageEngineConstants,
                lucenePageCacheStats,
                smallFilePageCache,
                bigFilePageCache,
                indexUniqueIdInRowGroup,
                luceneFileType,
                0,
                fileLengths[luceneFileType.getFileId()],
                "root");
    }

    @Override
    public void close()
    {
        logger.debug("indexUniqueIdInRowGroup=%d, close while input", indexUniqueIdInRowGroup);
    }

    @Override
    public Set<String> getPendingDeletions()
    {
        logger.debug("indexUniqueIdInRowGroup=%d, getPendingDeletions", indexUniqueIdInRowGroup);
        return null;
    }
}
