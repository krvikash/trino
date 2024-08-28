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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.gen.stats.LuceneIndexerStats;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.TrinoException;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.BaseEncoding.base64;
import static io.trino.plugin.warp.WarpErrorCode.WARP_LUCENE_FAILURE;
import static io.trino.plugin.warp.WarpErrorCode.WARP_LUCENE_WRITER_ERROR;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.util.SliceUtils.serializeSlice;

public class LuceneIndexer
        implements Closeable
{
    public static final Slice LUCENE_NULL_STRING = Slices.wrappedBuffer(base64().decode("27d52991a99c455789ed5e39b77226c8"));
    static final String VALUE_FIELD_NAME = "value";

    private static final Logger logger = Logger.get(LuceneIndexer.class);

    private final Analyzer analyzer = new KeywordAnalyzer();
    private final Document doc = new Document();
    private final List<ChunkState> chunkStates = new ArrayList<>();

    private final StorageEngineConstants storageEngineConstants;
    private final String rowGroupFilePath;
    private final Path path;
    private final LuceneIndexerStats stats;
    private final StopWatch stopWatch;

    private IndexWriter indexWriter;
    private String failedDocumentError;
    private boolean failedCommit;

    public LuceneIndexer(StorageEngineConstants storageEngineConstants, String rowGroupFilePath, LuceneIndexerStats stats)
    {
        this.storageEngineConstants = storageEngineConstants;
        this.rowGroupFilePath = rowGroupFilePath;
        this.path = Path.of(rowGroupFilePath.substring(0, rowGroupFilePath.lastIndexOf('/')),
                rowGroupFilePath.substring(rowGroupFilePath.lastIndexOf('/') + 1) + "-lucene");
        this.stats = stats;
        this.stopWatch = new StopWatch();
    }

    public void addDoc(Slice... values)
            throws IOException
    {
        if (failedCommit) {
            return;
        }

        try {
            stopWatch.reset();
            stopWatch.start();
            doc.clear();
            for (Slice value : values) {
                TextField textField = new TextField(VALUE_FIELD_NAME, serializeSlice(value), Field.Store.NO);
                doc.add(textField);
            }

            indexWriter.addDocument(doc);
            stopWatch.stop();
            stats.addaddDoc(stopWatch.getNanoTime());
        }
        catch (Exception e) {
            logger.error("got exception from addDocument - %s", e);
            failedDocumentError = e.getMessage();
            failedCommit = true;
            stats.incfailedAddDoc();
            throw e;
        }
    }

    public void resetLuceneIndex()
    {
        logger.debug("reset index");
        try {
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
            logDocMergePolicy.setNoCFSRatio(1.0);
            logDocMergePolicy.setMinMergeDocs(1_000_000);

            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setCodec(new Lucene99Codec(Lucene99Codec.Mode.BEST_SPEED));
            config.setUseCompoundFile(true);
            config.setMergePolicy(logDocMergePolicy);

            File dir = path.toFile();
            if (dir.exists()) {
                FileUtils.cleanDirectory(dir);
            }
            logger.debug("resetLuceneIndex path %s", path);
            indexWriter = new IndexWriter(FSDirectory.open(path), config);
        }
        catch (Exception e) {
            logger.warn("Got exception when creating the indexWriter - %s", e);
            stats.incfailedReset();
            throw new TrinoException(WarpErrorCode.WARP_LUCENE_WRITER_ERROR, "failed creating index writer of col", e);
        }
    }

    public void closeLuceneIndex(long[] fileCookieParams)
    {
        if (indexWriter == null) {
            return;
        }
        logger.debug("close index");
        try {
            stopWatch.reset();
            stopWatch.start();
            indexWriter.forceMerge(1);
            stopWatch.stop();
            stats.addmerge(stopWatch.getTime());
            indexWriter.close();

            if (failedCommit) {
                if (failedDocumentError != null) {
                    throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed on at least one document " + failedDocumentError);
                }
                else {
                    throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed before closing");
                }
            }

            saveLuceneIndex(fileCookieParams);
        }
        catch (Exception e) {
            throw new TrinoException(WARP_LUCENE_WRITER_ERROR, "Got exception when closing the indexWriter", e);
        }
        finally {
            close(indexWriter.getDirectory());
        }
    }

    private void saveLuceneIndex(long[] fileCookieParams)
    {
        int startOffset = (int) fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()];
        LuceneIndexWriter luceneIndexWriter = new LuceneIndexWriter(storageEngineConstants, indexWriter, rowGroupFilePath, startOffset);
        Optional<ChunkState> chunkState = luceneIndexWriter.saveLuceneIndex();

        chunkState.ifPresent(state -> {
            fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = state.endOffset();
            chunkStates.add(state);
        });
    }

    public int saveLuceneIndexState(int startOffset)
    {
        ChunkStateHandler chunkStateHandler = new ChunkStateHandler(storageEngineConstants, rowGroupFilePath, startOffset);
        return chunkStateHandler.save(chunkStates);
    }

    private void close(Directory directory)
    {
        stopWatch.reset();
        stopWatch.start();
        try {
            directory.close();
        }
        catch (IOException e) {
            logger.info(e.getMessage());
        }
        stopWatch.stop();
    }

    @Override
    public void close()
            throws IOException
    {
        File dir = path.toFile();
        if (dir.exists()) {
            FileUtils.deleteDirectory(dir);
        }
    }

    public void abort()
    {
        if (indexWriter != null) {
            close(indexWriter.getDirectory());
        }
    }

    @VisibleForTesting
    public Directory getLuceneDirectory()
    {
        return indexWriter.getDirectory();
    }
}
