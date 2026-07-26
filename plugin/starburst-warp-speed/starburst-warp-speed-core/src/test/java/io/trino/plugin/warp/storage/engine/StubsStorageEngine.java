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
import io.trino.plugin.warp.storage.write.WarmUpState;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class StubsStorageEngine
        implements StorageEngine
{
    public StubsStorageEngine() {}

    @Override
    public int getWarmupRecordBufferSize(int recTypeLength)
    {
        return 8192 * recTypeLength;
    }

    @Override
    public int getFixedCollectRecordBufferSize(int recTypeLength)
    {
        return 8192 * recTypeLength;
    }

    @Override
    public int getVarlenCollectRecordBufferSize(int recTypeLength)
    {
        return 8192 * recTypeLength;
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
    public void fileClose(int fileDescriptor) {}

    @Override
    public void fileTruncate(int fileDescriptor, int offset) {}

    @Override
    public void filePunchHole(String fileName, int startOffset, int endOffset) {}

    @Override
    public void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages) {}

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
    public void warmupVerifyQueryOffset(MemorySegment warmUpState) {}

    @Override
    public void warmupChunk(MemorySegment warmUpState, MemorySegment recordBufferParams, MemorySegment compressionState)
    {
        long chunkHeaderAddress = warmUpState.get(ValueLayout.JAVA_LONG, WarmUpState.WARMUP_STATE_OFFSET_CHUNK_HEADER);
        MemorySegment chunkHeader = MemorySegment.ofAddress(chunkHeaderAddress).reinterpret(ChunkHeader.CHUNK_HEADER_LAYOUT.byteSize());
        chunkHeader.set(ValueLayout.JAVA_BYTE, ChunkHeader.CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID, (byte) 0x66);
    }

    @Override
    public void matchOpen(MemorySegment matchState) {}

    @Override
    public int matchAgg(MemorySegment matchState, int startChunkIndex)
    {
        return 0;
    }

    @Override
    public boolean matchLucenePrepare(MemorySegment matchState, int weIx, int chunkIndex)
    {
        return true;
    }

    @Override
    public void matchLuceneCompleted(MemorySegment matchState, int weIx, int chunkIndex, int numMatchedRecords) {}

    @Override
    public boolean match(MemorySegment matchState, int startChunkIndex, int numChunks)
    {
        return true;
    }

    @Override
    public void matchClose(MemorySegment matchState) {}

    @Override
    public void collectOpen(MemorySegment collectState) {}

    @Override
    public boolean openChunk(MemorySegment collectState, int chunkIndex)
    {
        return true;
    }

    @Override
    public void collectChunk(MemorySegment collectState, MemorySegment outQueryResultTypes) {}

    @Override
    public void collectClose(MemorySegment collectState, MemorySegment readStats) {}

    @Override
    public boolean isLoaded()
    {
        return true;
    }

    @Override
    public boolean isFirstLoaded()
    {
        return true;
    }
}
