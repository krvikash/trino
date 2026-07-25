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
package io.trino.plugin.warp.storage.juffers;

import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.type.TypeUtils;

import java.lang.foreign.MemorySegment;

public class RecordWriteJuffer
        extends BaseWriteJuffer
{
    private final WarmUpElementAllocationParams allocParams;
    private final StorageEngine storageEngine;
    private final MemorySegment warmUpState;
    private final MemorySegment compressionState;
    private int recordBufferEntrySize;            // size of one record, one if its a byte buffer

    public RecordWriteJuffer(
            BufferAllocator bufferAllocator,
            WarmUpElementAllocationParams allocParams,
            StorageEngine storageEngine,
            MemorySegment warmUpState,
            MemorySegment compressionState)
    {
        super(bufferAllocator, JuffersType.RECORD);
        this.allocParams = allocParams;
        this.storageEngine = storageEngine;
        this.warmUpState = warmUpState;
        this.compressionState = compressionState;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs)
    {
        baseBuffer = bufferAllocator.memorySegment2RecBuff(buffs);
        RecTypeCode bufferRecTypeCode = allocParams.recTypeCode();
        int bufferRecTypeLength = allocParams.recTypeLength();
        this.wrappedBuffer = createWrapperBuffer(baseBuffer, bufferRecTypeCode, bufferRecTypeLength, true);
        this.recordBufferEntrySize = calcRecordBufferEntrySize(bufferRecTypeCode, bufferRecTypeLength);
    }

    public int getRecordBufferEntrySize()
    {
        return recordBufferEntrySize;
    }

    public void resetSingleRecordBufferPos()
    {
        wrappedBuffer.position(0);
    }

    private int calcRecordBufferEntrySize(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (!TypeUtils.isStr(recTypeCode) && ((recTypeLength == Short.BYTES) || (recTypeLength == Integer.BYTES) || (recTypeLength == Long.BYTES))) {
            return recTypeLength;
        }
        return 1;
    }

    public void commitAndResetWE(MemorySegment recordBufferParams)
    {
        // no need to add to chunk map as we are not closing the chunk
        storageEngine.warmupChunk(warmUpState, recordBufferParams, compressionState);
        resetSingleRecordBufferPos();
    }
}
