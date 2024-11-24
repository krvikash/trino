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

import io.trino.plugin.warp.storage.juffers.ChunkHeader;
import io.trino.plugin.warp.storage.read.StorageCollectorCallBack;
import io.trino.plugin.warp.storage.write.WarmUpState;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StubsStorageEngine
        implements StorageEngine
{
    private final List<RuntimeException> throwOnColletRuntimeExceptionList = new ArrayList<>();

    public StubsStorageEngine()
    {
    }

    @Override
    public int getWarmupRecordBufferSize(int recTypeLength)
    {
        return 1024 * recTypeLength;
    }

    @Override
    public int getFixedCollectRecordBufferSize(int recTypeLength)
    {
        return 1024 * recTypeLength;
    }

    @Override
    public int getVarlenCollectRecordBufferSize(int recTypeLength)
    {
        return 1024 * recTypeLength;
    }

    @Override
    public int getFixedCollectTxSize(int recTypeLength)
    {
        return 16 * recTypeLength;
    }

    @Override
    public int getVarlenCollectTxSize(int recTypeLength)
    {
        return 16 * recTypeLength;
    }

    @Override
    public int getFixedWarmupDataTxSize(int recTypeLength)
    {
        return 16 * recTypeLength;
    }

    @Override
    public int getVarlenWarmupDataTxSize(int recTypeLength)
    {
        return 16 * recTypeLength;
    }

    @Override
    public int getWarmupBasicTxSize()
    {
        return 16;
    }

    @Override
    public int getWarmupLuceneTxSize()
    {
        return 16;
    }

    @Override
    public int fileOpen(String fileName)
    {
        return 0;
    }

    @Override
    public void fileClose(int fileDescriptor)
    {
    }

    @Override
    public void fileTruncate(int fileDescriptor, int offset)
    {
    }

    @Override
    public void filePunchHole(String fileName, int startOffset, int endOffset)
    {
    }

    @Override
    public void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages)
    {
    }

    @Override
    public void warmupElementOpen(MemorySegment warmUpState, MemorySegment context)
    {
        warmUpState.set(ValueLayout.JAVA_INT, WarmUpState.WARMUP_STATE_OFFSET_START_OFFSET, 0);
    }

    @Override
    public int warmupElementClose(MemorySegment warmUpState)
    {
        warmUpState.set(ValueLayout.JAVA_INT, WarmUpState.WARMUP_STATE_OFFSET_START_OFFSET, 1);
        return 1;
    }

    @Override
    public void warmupVerifyQueryOffset(MemorySegment warmUpState)
    {
    }

    @Override
    public void warmupChunk(MemorySegment warmUpState, MemorySegment recordBufferParams, MemorySegment compressionState)
    {
        long chunkHeaderAddress = warmUpState.get(ValueLayout.JAVA_LONG, WarmUpState.WARMUP_STATE_OFFSET_CHUNK_HEADER);
        MemorySegment chunkHeader = MemorySegment.ofAddress(chunkHeaderAddress).reinterpret(ChunkHeader.CHUNK_HEADER_LAYOUT.byteSize());
        chunkHeader.set(ValueLayout.JAVA_BYTE, ChunkHeader.CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID, (byte) 0x66);
    }

    @Override
    public void warmupChunkExtRec(MemorySegment warmUpState, MemorySegment recordBufferParams)
    {
        long chunkHeaderAddress = warmUpState.get(ValueLayout.JAVA_LONG, WarmUpState.WARMUP_STATE_OFFSET_CHUNK_HEADER);
        MemorySegment chunkHeader = MemorySegment.ofAddress(chunkHeaderAddress).reinterpret(ChunkHeader.CHUNK_HEADER_LAYOUT.byteSize());
        chunkHeader.set(ValueLayout.JAVA_BYTE, ChunkHeader.CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID, (byte) 0x22);
    }

    @Override
    public long queryGetCollectStateSize(int numMatchCollect)
    {
        return 0;
    }

    @Override
    public void collectOpen(int totalNumRecords, long[] fileCookie, int collectTxId, byte[] parsingBuff,
            int numCollectWes, int numChunksInRange, int[] weCollectParams, long warmUpElementAttsAddress, long catalogContext, int minOffset,
            long matchBitmapAddress, long recordBufferStatesAddress, long recordIndexesAddress, long matchCollectMetadataAddress, long stateAddress,
            long[][] collectBuffers)
    {
    }

    @Override
    public long matchOpen(int totalNumRecords, long[] fileCookie, int collectTxId, byte[] parsingBuff, long matchCollectMetadataAddresss,
            int numMatchWes, int numChunksInRange, int[] weMatchTree, long warmUpElementAttsAddress, long matchBitmapAddress, long luceneBitmapAddress,
            int matchCollectId, int minOffset)
    {
        return 0;
    }

    @Override
    public long collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj)
    {
        return 0;
    }

    @Override
    public long matchAgg(int txId, int startChunkIndex)
    {
        return 0L;
    }

    @Override
    public long matchLucenePrepare(int matchTxId, int matchWeIx, int startChunkIndex, int numChunks, long[] outParams)
    {
        return 0L;
    }

    @Override
    public void matchLuceneCompleted(int matchTxId, int matchWeIx, int startChunkIndex, int numChunks, int[] matchResult)
    {
    }

    @Override
    public long match(int txId, int startChunkIndex, int numChunks, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints)
    {
        return 0L;
    }

    @Override
    public boolean processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit, MemorySegment outQueryResultTypes)
    {
        return true;
    }

    @Override
    public long processFullScanChunk(int txId, int chunkIndex, int startRowIx, int rowsLimit)
    {
        return 0;
    }

    @Override
    public void collectChunk(int txId, int numWes, int chunkIndex, int numToCollect, MemorySegment outQueryResultTypes)
    {
        if (!throwOnColletRuntimeExceptionList.isEmpty()) {
            throw throwOnColletRuntimeExceptionList.removeFirst();
        }
    }

    @Override
    public void collectClose(int txId, int[] chunksWithBitmapsToStore, int numChunksWithBitmaps, StorageCollectorCallBack obj, long[] outCollectStats)
    {
    }

    @Override
    public void matchClose(int txId)
    {
    }

    @Override
    public void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio)
    {
    }

    @Override
    public String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues)
    {
        return "";
    }

    @Override
    public boolean isLoaded()
    {
        return true;
    }

    public synchronized void clear()
    {
        throwOnColletRuntimeExceptionList.clear();
    }

    public void setThrowOnCollect(RuntimeException... e)
    {
        throwOnColletRuntimeExceptionList.addAll(Arrays.asList(e));
    }

    @Override
    public void cleanStorageCache()
    {
    }
}
