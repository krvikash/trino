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
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.gen.stats.LuceneIndexerStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.TrinoException;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
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
import static io.trino.plugin.warp.util.SliceUtils.serializeSlice;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class LuceneIndexer
        implements Closeable
{
    public static final Slice LUCENE_NULL_STRING = Slices.wrappedBuffer(base64().decode("27d52991a99c455789ed5e39b77226c8"));
    private static final int SIZE_LIMIT_DOCS_COUNT = 512;
    private static final int MAX_TMP_FILE_NAME = 200;
    static final String VALUE_FIELD_NAME = "value";

    private static final Logger logger = Logger.get(LuceneIndexer.class);
    private final ShapingLogger shapingLogger;

    private final Analyzer analyzer = new KeywordAnalyzer();
    private final Document doc = new Document();
    private final List<ChunkState> chunkStates = new ArrayList<>();

    private final StorageEngineConstants storageEngineConstants;
    private final ShapingLoggerFactory shapingLoggerFactory;

    private final String rowGroupFilePath;
    private final Path path;
    private final LuceneIndexerStats stats;
    private final StopWatch stopWatch;

    private IndexWriter indexWriter;
    private LuceneIndexWriter luceneIndexWriter;
    private String failedDocumentError;
    private boolean failedCommit;
    private int countDocsForSizeLimit;

    public LuceneIndexer(
            StorageEngineConstants storageEngineConstants,
            ShapingLoggerFactory shapingLoggerFactory,
            String rowGroupFilePath,
            LuceneIndexerStats stats)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
        this.rowGroupFilePath = rowGroupFilePath;

        String uniqueName = rowGroupFilePath;
        for (int i = 0; i < RowGroupKey.FILE_NAME_START_OF_FILE_NAME; i++) {
            uniqueName = uniqueName.substring(uniqueName.indexOf('/') + 1);
        }
        if (uniqueName.length() > MAX_TMP_FILE_NAME) {
            uniqueName = uniqueName.substring(uniqueName.length() - MAX_TMP_FILE_NAME);
        }
        this.path = Path.of("/tmp", uniqueName.replaceAll("/", "-"));

        this.stats = stats;
        this.stopWatch = new StopWatch();

        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    public void addDoc(Slice... values)
    {
        if (failedCommit) {
            return;
        }

        try {
            stopWatch.reset();
            stopWatch.start();

            doc.clear();

            for (Slice value : values) {
                String valueAsString = serializeSlice(value);

                if (valueAsString.getBytes(UTF_8).length > 32766) {
                    failedDocumentError = "failed creating doc (path %s) - UTF8 encoding is longer than max length 32766".formatted(path);
                    failedCommit = true;
                    return;
                }
                TextField textField = new TextField(VALUE_FIELD_NAME, valueAsString, Field.Store.NO);
                doc.add(textField);
            }

            indexWriter.addDocument(doc);

            countDocsForSizeLimit++;
            if (countDocsForSizeLimit == SIZE_LIMIT_DOCS_COUNT) {
                if (luceneIndexWriter.isBigFileSizeExceededMax()) {
                    failedCommit = true;
                }
                countDocsForSizeLimit = 0;
            }
            stopWatch.stop();
            stats.addaddDoc(stopWatch.getNanoTime());
        }
        catch (Exception e) {
            failedDocumentError = "Got exception from addDocument (path %s) - %s".formatted(path, e);
            shapingLogger.error(failedDocumentError);
            failedCommit = true;
        }
        finally {
            if (failedCommit) {
                stats.incfailedAddDoc();
                closeLuceneIndex(); // will throw an exception
            }
        }
    }

    public void resetLuceneIndex()
    {
        try {
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
            logDocMergePolicy.setNoCFSRatio(1.0);
            logDocMergePolicy.setMinMergeDocs(1_000_000);

            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setUseCompoundFile(true);
            config.setMergePolicy(logDocMergePolicy);

            File dir = path.toFile();
            if (dir.exists()) {
                FileUtils.cleanDirectory(dir);
            }
            logger.debug("create temporary directory %s", path);
            indexWriter = new IndexWriter(FSDirectory.open(path), config);
            luceneIndexWriter = new LuceneIndexWriter(storageEngineConstants, indexWriter, rowGroupFilePath, shapingLoggerFactory);
        }
        catch (Exception e) {
            logger.warn("Got exception when creating the indexWriter - %s", e);
            stats.incfailedReset();
            throw new TrinoException(WarpErrorCode.WARP_LUCENE_WRITER_ERROR, "failed creating index writer of col", e);
        }
    }

    private void closeLuceneIndexUnsafe()
            throws IOException
    {
        stopWatch.reset();
        stopWatch.start();
        indexWriter.forceMerge(1);
        stopWatch.stop();
        stats.addmerge(stopWatch.getTime());
        indexWriter.close();
    }

    private void closeLuceneIndex()
    {
        try {
            closeLuceneIndexUnsafe();

            if (failedCommit) {
                if (failedDocumentError != null) {
                    throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed on at least one document " + failedDocumentError);
                }
                else {
                    throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed before closing");
                }
            }
        }
        catch (Exception e) {
            throw new TrinoException(WARP_LUCENE_WRITER_ERROR, "Got exception when closing the indexWriter", e);
        }
        finally {
            closeDirectory(indexWriter.getDirectory());
        }
    }

    public int closeAndSaveLuceneIndex(int startOffset)
    {
        if (indexWriter == null) {
            return startOffset;
        }
        try {
            closeLuceneIndexUnsafe();

            if (failedCommit) {
                if (failedDocumentError != null) {
                    throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed on at least one document " + failedDocumentError);
                }
                else {
                    throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed before closing");
                }
            }

            int endOffset = saveLuceneIndex(startOffset);
            if (endOffset < 0) {
                logger.warn("lucene index failed on file too big rowGroupFilePath %s", rowGroupFilePath);
                throw new TrinoException(WARP_LUCENE_FAILURE, "lucene index failed on file too big");
            }
            return endOffset;
        }
        catch (Exception e) {
            throw new TrinoException(WARP_LUCENE_WRITER_ERROR, "Got exception when closing the indexWriter", e);
        }
        finally {
            closeDirectory(indexWriter.getDirectory());
        }
    }

    private int saveLuceneIndex(int startOffset)
    {
        Optional<ChunkState> chunkState = luceneIndexWriter.saveLuceneIndex(startOffset);

        if (chunkState.isEmpty()) {
            return -1;
        }
        ChunkState state = chunkState.get();
        chunkStates.add(state);
        return state.endOffset();
    }

    public int saveLuceneIndexState(int startOffset)
    {
        ChunkStateHandler chunkStateHandler = new ChunkStateHandler(storageEngineConstants, rowGroupFilePath, startOffset);
        return chunkStateHandler.save(chunkStates);
    }

    private void closeDirectory(Directory directory)
    {
        stopWatch.reset();
        stopWatch.start();
        try {
            directory.close();
            logger.debug("closed temporary directory %s", path);
            close();
        }
        catch (IOException e) {
            logger.warn(e.getMessage());
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
            logger.debug("removed temporary directory %s", path);
        }
    }

    public void abort()
    {
        if (indexWriter != null) {
            closeDirectory(indexWriter.getDirectory());
        }
    }

    @VisibleForTesting
    public Directory getLuceneDirectory()
    {
        return indexWriter.getDirectory();
    }
}
