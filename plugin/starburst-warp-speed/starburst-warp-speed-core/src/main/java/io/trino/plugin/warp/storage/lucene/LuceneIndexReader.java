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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class LuceneIndexReader
{
    private static final Logger logger = Logger.get(LuceneIndexReader.class);

    private final StorageEngineConstants storageEngineConstants;
    private final String rowGroupFilePath;
    private final ChunkState chunkState;

    public LuceneIndexReader(StorageEngineConstants storageEngineConstants,
                             String rowGroupFilePath,
                             ChunkState chunkState)
    {
        this.storageEngineConstants = storageEngineConstants;
        this.rowGroupFilePath = rowGroupFilePath;
        this.chunkState = chunkState;
    }

    public ByteBuffer loadSmallFile(LuceneFileType luceneFileType)
            throws IOException
    {
        File rowGroupDataFile = new File(rowGroupFilePath);
        int[] filesLength = chunkState.filesLength();

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            long offset = Integer.toUnsignedLong(chunkState.startOffset()) * storageEngineConstants.getPageSize();

            for (int i = 0; i < luceneFileType.getFileId(); i++) {
                offset += filesLength[i];
            }
            randomAccessFile.seek(offset);

            int length = filesLength[luceneFileType.getFileId()];
            byte[] luceneBytes = new byte[length];
            int readBytes = randomAccessFile.read(luceneBytes);

            if (readBytes <= 0) {
                logger.error("loadSmallFile failed rowGroupFilePath %s chunkState %s luceneFileType %s readBytes %d",
                        rowGroupFilePath, chunkState, luceneFileType, readBytes);
                throw new RuntimeException("end of file reached");
            }
            logger.debug("loadSmallFile rowGroupFilePath %s chunkState %s luceneFileType %s readBytes %d",
                    rowGroupFilePath, chunkState, luceneFileType, readBytes);
            return ByteBuffer.wrap(luceneBytes);
        }
    }

    public ByteBuffer loadBigFilePage(int pageIndex)
            throws IOException
    {
        File rowGroupDataFile = new File(rowGroupFilePath);
        int[] filesLength = chunkState.filesLength();

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            // big file starts on 2nd page. 1st page is used for small files
            long offset = Integer.toUnsignedLong(chunkState.startOffset() + 1 + pageIndex) * storageEngineConstants.getPageSize();
            randomAccessFile.seek(offset);

            int length = filesLength[LuceneFileType.CFS.getFileId()] - (pageIndex * storageEngineConstants.getPageSize());
            int bufferSize = Math.min(length, storageEngineConstants.getPageSize());
            byte[] luceneBytes = new byte[bufferSize];

            int readBytes = randomAccessFile.read(luceneBytes);

            if (readBytes <= 0) {
                logger.error("loadBigFilePage failed rowGroupFilePath %s chunkState %s pageIndex %d length %d bufferSize %d readBytes %d",
                        rowGroupFilePath, chunkState, pageIndex, length, bufferSize, readBytes);
                throw new RuntimeException("end of file reached");
            }
            logger.debug("loadBigFilePage rowGroupFilePath %s chunkState %s pageIndex %d bufferSize %d readBytes %d",
                    rowGroupFilePath, chunkState, pageIndex, bufferSize, readBytes);
            return ByteBuffer.wrap(luceneBytes);
        }
    }
}
