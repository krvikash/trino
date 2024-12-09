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
    //----------------------- initialization ----------------------------------------
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

    //----------------------- file ----------------------------------------
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

    //----------------------- warmup ----------------------------------------

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

    default void warmupChunkExtRec(MemorySegment warmUpState, MemorySegment recordBufferParams)
    {
        throw new UnsupportedOperationException();
    }

    //----------------------- query ----------------------------------------
    default void matchOpen(MemorySegment matchState)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * match aggregates on a chunk range starting from the given chunk index
     *
     * @param matchState - match state
     * @param startChunkIndex - chunk to start match from
     *
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
     *
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
     * @param matchStateAddress - match state
     * @param startChunkIndex - chunk to start match from
     * @param numChunks - number of chunks to match
     * @param outMatchedChunksIndexes - indexes of chunks that have at least one match
     *
     * @return number of matched chunks as filled in the output array, in case of error we return -1L
     */
    default long match(long matchStateAddress, int startChunkIndex, int numChunks, short[] outMatchedChunksIndexes)
    {
        throw new UnsupportedOperationException();
    }

    default void matchClose(MemorySegment matchState)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * open a collect transaction
     *
     * @param totalNumRecords - total number of records in the warm up element
     * @param fileCookie - hot file to read from
     * @param collectTxId - transaction id
     * @param parsingBuff - buffer for native to parse the collect parameters
     * @param numCollectWes - number of collect warm up elements
     * @param weCollectParams - parameters for collect warmup elements dumped into an array
     * @param catalogContext - connector context used for callbacks handles
     * @param collectBuffers - buffer for data and nulls per warm up element
     */
    default void collectOpen(int totalNumRecords, long[] fileCookie, int collectTxId, byte[] parsingBuff, int numCollectWes, int numChunksInRange, int reopenChunkIndex,
            int[] weCollectParams, long warmUpElementAttsAddress, long catalogContext, int minOffset, boolean isFullScan,
            long recordBufferStatesAddress, long recordIndexesAddress, long matchCollectMetadataAddress, long[][] collectBuffers)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * process match result on a chunk before collect
     *
     * @param txId - identifies tx, passed from native to java during import_create
     * @param chunkIndex - chunk to collect from
     * @param bitmapDescriptor - match bitmap pointer and reset point
     * @param rowsLimit - optional limit on the number of rows to collect from this chunk
     *
     * @return TRUE for success, FALSE for error
     */
    default boolean processMatchResult(int txId, int chunkIndex, MemorySegment bitmapDescriptor, int rowsLimit, MemorySegment outQueryResultTypes)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * process full scan chunk before collect
     *
     * @param txId - identifies tx, passed from native to java during import_create
     * @param chunkIndex - chunk to collect from
     * @param startRowIx - start row in chunk
     * @param rowsLimit - optional limit on the number of rows to collect from this chunk
     *
     * @return > 0 if buffer is full and we need to close collect, 0 if not, -1 for error
     */
    default long processFullScanChunk(int txId, int chunkIndex, int startRowIx, int rowsLimit)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Collect from the provided chunk according to the processMatchResult result
     *
     * @param txId - identifies tx, passed from native to java during import_create
     * @param numWes - number of WEs to collect
     * @param chunkIndex - chunk to collect from
     * @param numToCollect - how many rows to collect
     * @param outQueryResultTypes - array to hold updated result type for each collected WE for java to process the collect buffers
     */
    default void collectChunk(int txId, int numWes, int chunkIndex, int numToCollect, MemorySegment outQueryResultTypes)
    {
        throw new UnsupportedOperationException();
    }

    default void collectClose(int txId, long[] outCollectStats)
    {
        throw new UnsupportedOperationException();
    }

    //----------------------- statistics and debug ----------------------------------------
    default void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio)
    {
        throw new UnsupportedOperationException();
    }

    default String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues)
    {
        throw new UnsupportedOperationException();
    }

    default boolean isLoaded()
    {
        return true;
    }

    default void cleanStorageCache()
    {
        throw new UnsupportedOperationException();
    }
}
