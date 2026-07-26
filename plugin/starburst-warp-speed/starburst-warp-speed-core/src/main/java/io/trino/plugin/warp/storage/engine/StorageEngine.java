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
package io.trino.plugin.warp.storage.engine;

import java.lang.foreign.MemorySegment;

/**
 * API with the storage engine implementation
 */
public interface StorageEngine
{
    // ----------------------- initialization ----------------------------------------
    default int getWarmupRecordBufferSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getFixedCollectRecordBufferSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getVarlenCollectRecordBufferSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getFixedCollectTxSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getVarlenCollectTxSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getFixedWarmupDataTxSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getVarlenWarmupDataTxSize(int recTypeLength)
    {
        throw new UnsupportedOperationException();
    }

    default int getWarmupBasicTxSize()
    {
        throw new UnsupportedOperationException();
    }

    default int getWarmupLuceneTxSize()
    {
        throw new UnsupportedOperationException();
    }

    // ----------------------- file ----------------------------------------
    default int fileOpen(String fileName)
    {
        throw new UnsupportedOperationException();
    }

    default void fileClose(int fileDescriptor)
    {
        throw new UnsupportedOperationException();
    }

    default void fileTruncate(int fileDescriptor, int offset)
    {
        throw new UnsupportedOperationException();
    }

    default void filePunchHole(String fileName, int startOffset, int endOffset)
    {
        throw new UnsupportedOperationException();
    }

    default void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages)
    {
        throw new UnsupportedOperationException();
    }

    // ----------------------- warmup ----------------------------------------

    default void warmupElementOpen(MemorySegment warmUpState, MemorySegment context)
    {
        throw new UnsupportedOperationException();
    }

    default int warmupElementClose(MemorySegment warmUpState)
    {
        throw new UnsupportedOperationException();
    }

    default void warmupVerifyQueryOffset(MemorySegment warmUpState)
    {
        throw new UnsupportedOperationException();
    }

    default void warmupChunk(MemorySegment warmUpState, MemorySegment recordBufferParams, MemorySegment compressionState)
    {
        throw new UnsupportedOperationException();
    }

    // ----------------------- query ----------------------------------------
    default void matchOpen(MemorySegment matchState)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * match aggregates on a chunk range starting from the given chunk index
     *
     * @param matchState - match state
     * @param startChunkIndex - chunk to start match from
     * @return 0 for no more chunks, >0 for success giving the number of chunks in range, -1 for error
     */
    default int matchAgg(MemorySegment matchState, int startChunkIndex)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * prepare for match lucene on a chunk range starting from the given chunk index
     *
     * @param matchState - match state
     * @param weIx - wearm up element index
     * @param chunkIndex - chunk index
     * @return TRUE for success, FALSE for error (to throw exception)
     */
    default boolean matchLucenePrepare(MemorySegment matchState, int weIx, int chunkIndex)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * cleanup after match lucene on a chunk range starting from the given chunk index
     *
     * @param matchState - match state
     * @param weIx - wearm up element index
     * @param chunkIndex - chunk index
     * @param numMatchedRecords - number of matched records
     */
    default void matchLuceneCompleted(MemorySegment matchState, int weIx, int chunkIndex, int numMatchedRecords)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * match on a chunk range starting from the given chunk index, filling the match bitmaps as output
     *
     * @param matchState - match state
     * @param startChunkIndex - chunk to start match from
     * @param numChunks - number of chunks to match
     * @return TRUE if there were no errors (even if no match), FALSE if there was an error
     */
    default boolean match(MemorySegment matchState, int startChunkIndex, int numChunks)
    {
        throw new UnsupportedOperationException();
    }

    default void matchClose(MemorySegment matchState)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * open a collect transaction
     */
    default void collectOpen(MemorySegment collectState)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * open chunk before collect
     *
     * @param collectState - collect state
     * @param chunkIndex - chunk to collect from
     * @return TRUE for success, FALSE for error
     */
    default boolean openChunk(MemorySegment collectState, int chunkIndex)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Collect records from the provided chunk
     *
     * @param collectState - collect state
     * @param outQueryResultTypes - array to hold updated result type for each collected WE for java to process the collect buffers
     */
    default void collectChunk(MemorySegment collectState, MemorySegment outQueryResultTypes)
    {
        throw new UnsupportedOperationException();
    }

    default void collectClose(MemorySegment collectState, MemorySegment readStats)
    {
        throw new UnsupportedOperationException();
    }

    default boolean isLoaded()
    {
        return true;
    }

    default boolean isFirstLoaded()
    {
        return true;
    }

    default void shutdown() {}
}
