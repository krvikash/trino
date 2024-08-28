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
import java.util.ArrayList;
import java.util.List;

public class ChunkStateHandler
{
    private static final Logger logger = Logger.get(ChunkStateHandler.class);

    private static final byte[] padding = new byte[8192];   // PageSize

    private final StorageEngineConstants storageEngineConstants;
    private final String rowGroupFilePath;
    private final int matchOffset;
    private final int numChunkStatesInPage;
    private final int paddingSize;

    public ChunkStateHandler(StorageEngineConstants storageEngineConstants, String rowGroupFilePath, int matchOffset)
    {
        this.storageEngineConstants = storageEngineConstants;
        this.rowGroupFilePath = rowGroupFilePath;
        this.matchOffset = matchOffset;
        // reserve 4 bytes on each page for number of chuck states in this page
        this.numChunkStatesInPage = (storageEngineConstants.getPageSize() - Integer.BYTES) / ChunkState.size();
        int pageRemainder = (Integer.BYTES + numChunkStatesInPage * ChunkState.size()) & storageEngineConstants.getPageOffsetMask();
        this.paddingSize = (pageRemainder != 0) ? (storageEngineConstants.getPageSize() - pageRemainder) : 0;
    }

    public int save(List<ChunkState> chunkStates)
    {
        File rowGroupDataFile = new File(rowGroupFilePath);
        int sizeInPages = 0;

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            long offset = Integer.toUnsignedLong(matchOffset) * storageEngineConstants.getPageSize();
            randomAccessFile.seek(offset);

            ByteBuffer byteBuffer = ByteBuffer.allocate(storageEngineConstants.getPageSize());

            for (int page = 0; page < chunkStates.size() / numChunkStatesInPage; page++) {
                byteBuffer.putInt(numChunkStatesInPage); // number of chuck states in this page

                for (int chunk = 0; chunk < numChunkStatesInPage; chunk++) {
                    chunkStates.get(chunk).put(byteBuffer);
                }

                if (paddingSize > 0) {
                    // write padding
                    byteBuffer.put(padding, 0, paddingSize);
                }

                // save page
                randomAccessFile.write(byteBuffer.array());
                sizeInPages++;
                byteBuffer.clear();
            }

            // save last page
            int remainingChunks = chunkStates.size() % numChunkStatesInPage;
            if (remainingChunks > 0) {
                int startChunk = sizeInPages * numChunkStatesInPage;
                byteBuffer.putInt(remainingChunks); // number of chuck states in this page

                for (int chunk = 0; chunk < remainingChunks; chunk++) {
                    chunkStates.get(startChunk + chunk).put(byteBuffer);
                }

                int pageRemainder = byteBuffer.position() & storageEngineConstants.getPageOffsetMask();
                int paddingSize = (pageRemainder != 0) ? (storageEngineConstants.getPageSize() - pageRemainder) : 0;

                if (paddingSize > 0) {
                    // write padding
                    byteBuffer.put(padding, 0, paddingSize);
                }

                // save page
                randomAccessFile.write(byteBuffer.array());
                sizeInPages++;
            }
        }
        catch (IOException e) {
            logger.error("save failed rowGroupFilePath %s matchOffset %d chunkStates.size %d message %s",
                    rowGroupFilePath, matchOffset, chunkStates.size(), e.getMessage());
            throw new RuntimeException(e);
        }
        logger.debug("save rowGroupFilePath %s matchOffset %d chunkStates.size %d sizeInPages %d",
                rowGroupFilePath, matchOffset, chunkStates.size(), sizeInPages);
        return sizeInPages;
    }

    // returns a list of chunk states from the requested page
    public List<ChunkState> load(int pageIndex)
    {
        List<ChunkState> chunkStates = new ArrayList<>();
        File rowGroupDataFile = new File(rowGroupFilePath);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            long offset = Integer.toUnsignedLong(matchOffset + pageIndex) * storageEngineConstants.getPageSize();
            randomAccessFile.seek(offset);

            byte[] bytes = new byte[storageEngineConstants.getPageSize()];
            int readBytes = randomAccessFile.read(bytes);

            if (readBytes <= 0) {
                logger.error("load failed rowGroupFilePath %s matchOffset %d pageIndex %d readBytes %d",
                        rowGroupFilePath, matchOffset, pageIndex, readBytes);
                throw new RuntimeException("end of file reached");
            }

            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            int size = byteBuffer.getInt();

            for (int i = 0; i < size; i++) {
                ChunkState chunkState = ChunkState.get(byteBuffer);
                chunkStates.add(chunkState);
            }
        }
        catch (IOException e) {
            logger.error("load failed rowGroupFilePath %s matchOffset %d chunkStates.size %d message %s",
                    rowGroupFilePath, matchOffset, chunkStates.size(), e.getMessage());
            throw new RuntimeException(e);
        }
        logger.debug("load rowGroupFilePath %s matchOffset %d pageIndex %d chunkStates.size %d",
                rowGroupFilePath, matchOffset, pageIndex, chunkStates.size());
        return chunkStates;
    }

    public int getPageIndex(int chunkIndex)
    {
        return chunkIndex / numChunkStatesInPage;
    }

    public int getChunkIndexInPage(int chunkIndex)
    {
        return chunkIndex % numChunkStatesInPage;
    }
}
