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
import java.util.Arrays;
import java.util.Optional;

public class LuceneIndexWriter
{
    private static final Logger logger = Logger.get(LuceneIndexWriter.class);

    private static final byte[] padding = new byte[8192];   // PageSize
    private static final int maxSaveSize = 512 * 1024;

    private final StorageEngineConstants storageEngineConstants;
    private final IndexWriter indexWriter;
    private final String rowGroupFilePath;
    private final int startOffset;

    public LuceneIndexWriter(StorageEngineConstants storageEngineConstants,
                             IndexWriter indexWriter,
                             String rowGroupFilePath,
                             int startOffset)
    {
        this.storageEngineConstants = storageEngineConstants;
        this.indexWriter = indexWriter;
        this.rowGroupFilePath = rowGroupFilePath;
        this.startOffset = startOffset;
    }

    public Optional<ChunkState> saveLuceneIndex()
    {
        if (indexWriter == null) {
            return Optional.empty();
        }

        int sizeInPages = 0;

        try {
            Directory luceneDirectory = indexWriter.getDirectory();
            String[] luceneFiles = luceneDirectory.listAll();
            int[] filesLength = getFilesLength(luceneDirectory, luceneFiles);
            File rowGroupDataFile = new File(rowGroupFilePath);

            try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
                long offset = Integer.toUnsignedLong(startOffset) * storageEngineConstants.getPageSize();
                randomAccessFile.seek(offset);

                // write padding page, will be overwritten later by the small files
                randomAccessFile.write(padding);

                for (String luceneFile : luceneFiles) {
                    LuceneFileType luceneFileType = LuceneFileType.getType(luceneFile);

                    if (luceneFileType != LuceneFileType.UNKNOWN) {
                        IOContext context = IOContext.READONCE;

                        try (IndexInput indexInput = luceneDirectory.openInput(luceneFile, context)) {
                            if (luceneFileType.isSmallFile()) {
                                saveSmallFile(indexInput, luceneFileType, filesLength, offset, randomAccessFile);
                            }
                            else {
                                // reserve one page for small files
                                sizeInPages += saveBigFile(indexInput, startOffset + 1, randomAccessFile);
                            }
                        }
                    }
                }
            }
            ChunkState chunkState = new ChunkState(startOffset, sizeInPages + 1, filesLength);
            logger.debug("saveLuceneIndex rowGroupFilePath %s chunkState %s", rowGroupFilePath, chunkState);
            return Optional.of(chunkState);
        }
        catch (IOException e) {
            logger.error("saveLuceneIndex failed rowGroupFilePath %s startOffset %d message %s",
                    rowGroupFilePath, startOffset, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private int[] getFilesLength(Directory luceneDirectory, String[] luceneFiles)
            throws IOException
    {
        int[] filesLength = new int[LuceneFileType.values().length - 1];

        for (String luceneFile : luceneFiles) {
            LuceneFileType luceneFileType = LuceneFileType.getType(luceneFile);

            if (luceneFileType != LuceneFileType.UNKNOWN) {
                IOContext context = IOContext.READONCE;

                try (IndexInput indexInput = luceneDirectory.openInput(luceneFile, context)) {
                    filesLength[luceneFileType.getFileId()] = (int) indexInput.length();
                }
            }
        }
        logger.debug("getFilesLength rowGroupFilePath %s filesLength %s", rowGroupFilePath, Arrays.toString(filesLength));
        return filesLength;
    }

    private void saveSmallFile(IndexInput indexInput, LuceneFileType luceneFileType, int[] filesLength, long offset, RandomAccessFile randomAccessFile)
            throws IOException
    {
        int fileOffset = 0;

        for (int i = 0; i < luceneFileType.getFileId(); i++) {
            fileOffset += filesLength[i];
        }
        randomAccessFile.seek(offset + fileOffset);

        int length = saveFile(indexInput, randomAccessFile);

        logger.debug("saveSmallFile rowGroupFilePath %s luceneFileType %s offset %d (%d) fileOffset %d length %d",
                rowGroupFilePath, luceneFileType, offset, offset / storageEngineConstants.getPageSize(), fileOffset, length);
    }

    private int saveBigFile(IndexInput indexInput, int pageOffset, RandomAccessFile randomAccessFile)
            throws IOException
    {
        long offset = Integer.toUnsignedLong(pageOffset) * storageEngineConstants.getPageSize();
        randomAccessFile.seek(offset);

        int length = saveFile(indexInput, randomAccessFile);

        int pageRemainder = length & storageEngineConstants.getPageOffsetMask();
        int paddingSize = (pageRemainder != 0) ? (storageEngineConstants.getPageSize() - pageRemainder) : 0;

        if (paddingSize > 0) {
            // write padding
            randomAccessFile.write(padding, 0, paddingSize);
            length += paddingSize;
        }

        int sizeInPages = length / storageEngineConstants.getPageSize();
        logger.debug("saveBigFile rowGroupFilePath %s pageOffset %d length %d paddingSize %d sizeInPages %d",
                rowGroupFilePath, pageOffset, length, paddingSize, sizeInPages);
        return sizeInPages;
    }

    private int saveFile(IndexInput indexInput, RandomAccessFile randomAccessFile)
            throws IOException
    {
        int length = (int) indexInput.length();
        int bufferSize = Math.min(length, maxSaveSize);
        byte[] luceneBytes = new byte[bufferSize];
        int bytesLeft = length;

        while (bytesLeft > 0) {
            int bytesToRead = Math.min(bufferSize, bytesLeft);

            indexInput.readBytes(luceneBytes, 0, bytesToRead);
            randomAccessFile.write(luceneBytes, 0, bytesToRead);

            bytesLeft -= bytesToRead;
        }
        return length;
    }
}
