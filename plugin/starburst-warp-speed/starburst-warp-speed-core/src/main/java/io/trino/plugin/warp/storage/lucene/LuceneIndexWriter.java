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
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;

public class LuceneIndexWriter
{
    private static final Logger logger = Logger.get(LuceneIndexWriter.class);

    private static final int BIG_FILE_BUFFER_SIZE = 512 * 1024;     // 0.5 MB
    private static final int MAX_TOTAL_FILE_SIZES = 64 * 1024 * 1024; // 128 MB

    private final StorageEngineConstants storageEngineConstants;
    private final IndexWriter indexWriter;
    private final String rowGroupFilePath;

    public LuceneIndexWriter(StorageEngineConstants storageEngineConstants, IndexWriter indexWriter, String rowGroupFilePath)
    {
        this.storageEngineConstants = storageEngineConstants;
        this.indexWriter = indexWriter;
        this.rowGroupFilePath = rowGroupFilePath;
    }

    public Optional<ChunkState> saveLuceneIndex(int startOffset)
    {
        if (indexWriter == null) {
            return Optional.empty();
        }

        try {
            Directory luceneDirectory = indexWriter.getDirectory();
            String[] luceneFiles = luceneDirectory.listAll();
            int[] filesLength = new int[LuceneFileType.values().length - 1];
            File rowGroupDataFile = new File(rowGroupFilePath);
            int bigFileSizeAligned = 0;

            try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
                long startOffsetInBytes = Integer.toUnsignedLong(startOffset) * storageEngineConstants.getPageSize();
                String luceneBigFile = null;
                LuceneFileType luceneBigFileType = LuceneFileType.UNKNOWN;
                IOContext context = IOContext.READONCE;
                int totalFileSizes = 0;

                // read all small files into one page and gather total size
                byte[] smallFilesPage = new byte[storageEngineConstants.getPageSize()];
                int smallFileSize = smallFilesPage.length / (filesLength.length - 1);
                for (String luceneFile : luceneFiles) {
                    LuceneFileType luceneFileType = LuceneFileType.getType(luceneFile);
                    if (luceneFileType != LuceneFileType.UNKNOWN) {
                        try (IndexInput indexInput = luceneDirectory.openInput(luceneFile, context)) {
                            int length = (int) indexInput.length();
                            totalFileSizes += length;
                            if (totalFileSizes > MAX_TOTAL_FILE_SIZES) {
                                logger.warn("lucene index exceeded size limit on size %d rowGroupFilePath %s", totalFileSizes, rowGroupFilePath);
                                return Optional.empty();
                            }
                            if (!luceneFileType.isSmallFile()) {
                                luceneBigFile = luceneFile;
                                luceneBigFileType = luceneFileType;
                                continue;
                            }
                            indexInput.readBytes(smallFilesPage, smallFileSize * luceneFileType.getFileId(), length);
                            filesLength[luceneFileType.getFileId()] = length;
                        }
                    }
                }
                randomAccessFile.seek(startOffsetInBytes);
                randomAccessFile.write(smallFilesPage, 0, smallFilesPage.length);

                // big file
                try (IndexInput indexInput = luceneDirectory.openInput(luceneBigFile, context)) {
                    int length = (int) indexInput.length();
                    int bytesLeft = length;
                    int pageRemainder = length & storageEngineConstants.getPageOffsetMask();
                    int paddingSize = (pageRemainder != 0) ? (storageEngineConstants.getPageSize() - pageRemainder) : 0;
                    bigFileSizeAligned = length + paddingSize;

                    byte[] bigFilesBuffer = new byte[Math.min(bigFileSizeAligned, BIG_FILE_BUFFER_SIZE)];
                    // full buffer rounds
                    while (bytesLeft > BIG_FILE_BUFFER_SIZE) {
                        indexInput.readBytes(bigFilesBuffer, 0, BIG_FILE_BUFFER_SIZE);
                        randomAccessFile.write(bigFilesBuffer, 0, BIG_FILE_BUFFER_SIZE);
                        bytesLeft -= BIG_FILE_BUFFER_SIZE;
                    }
                    // last round - we add the padding to page size
                    indexInput.readBytes(bigFilesBuffer, 0, bytesLeft);
                    randomAccessFile.write(bigFilesBuffer, 0, bytesLeft + paddingSize);
                    // update the length
                    filesLength[luceneBigFileType.getFileId()] = length;
                }
            }
            ChunkState chunkState = new ChunkState(startOffset, (bigFileSizeAligned / storageEngineConstants.getPageSize()) + 1, filesLength);
            logger.debug("saveLuceneIndex rowGroupFilePath %s chunkState %s", rowGroupFilePath, chunkState);
            return Optional.of(chunkState);
        }
        catch (IOException e) {
            logger.error("saveLuceneIndex failed rowGroupFilePath %s startOffset %d message %s", rowGroupFilePath, startOffset, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean isBigFileSizeExceededMax()
    {
        try {
            // find the big file
            Directory luceneDirectory = indexWriter.getDirectory();
            String[] luceneFiles = luceneDirectory.listAll();
            int totalFileSizes = 0;
            IOContext context = IOContext.READONCE;
            for (String luceneFile : luceneFiles) {
                LuceneFileType luceneFileType = LuceneFileType.getType(luceneFile);
                if (luceneFileType != LuceneFileType.UNKNOWN) {
                    try (IndexInput indexInput = luceneDirectory.openInput(luceneFile, context)) {
                        totalFileSizes += (int) indexInput.length();
                    }
                }
            }
            if (totalFileSizes > MAX_TOTAL_FILE_SIZES) {
                logger.warn("lucene big file exceeded size limit on size %d rowGroupFilePath %s", totalFileSizes, rowGroupFilePath);
                return true;
            }
            return false;
        }
        catch (IOException e) {
            logger.error("lucene big file size check failed rowGroupFilePath %s message %s", rowGroupFilePath, e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
