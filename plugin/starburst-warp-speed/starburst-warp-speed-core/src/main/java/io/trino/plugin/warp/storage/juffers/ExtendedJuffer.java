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

import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.warp.storage.engine.StorageEngine;

import java.lang.foreign.MemorySegment;

/**
 * buffer for marking extended strings
 */
public class ExtendedJuffer
        extends BaseWriteJuffer
{
    private final WarmUpElementAllocationParams allocParams;
    private final StorageEngine storageEngine;
    private final MemorySegment warmUpState;
    private final RecordBufferParams recordBufferParams;
    private int extSize;                            // extended recs buffer size
    private int extRecFirstOffset;                  // offset of the first extended entry we encountered
    private int extRecLastPos;                      // extended records last entry address

    public ExtendedJuffer(
            BufferAllocator bufferAllocator,
            WarmUpElementAllocationParams allocParams,
            StorageEngine storageEngine,
            MemorySegment warmUpState,
            RecordBufferParams recordBufferParams)
    {
        super(bufferAllocator, JuffersType.EXTENDED_REC);
        this.allocParams = allocParams;
        this.storageEngine = storageEngine;
        this.warmUpState = warmUpState;
        this.recordBufferParams = recordBufferParams;
        extRecFirstOffset = -1;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.memorySegment2ExtRecsBuff(buffs));
        this.wrappedBuffer = this.baseBuffer;
        this.extSize = allocParams.extRecBuffSize();
    }

    protected void commitAndResetExtRecordBuffer(int numExtBytes)
    {
        if (numExtBytes > 0) {
            recordBufferParams.setExtParams(numExtBytes, extRecFirstOffset);
            storageEngine.warmupChunkExtRec(warmUpState, recordBufferParams.getMemory());
            resetExtBuf();
        }
    }

    public void commitAndResetExtRecordBuffer()
    {
        if (wrappedBuffer.position() > 0) {
            recordBufferParams.setExtParams(wrappedBuffer.position(), extRecFirstOffset);
            storageEngine.warmupChunkExtRec(warmUpState, recordBufferParams.getMemory());
        }
        resetExtBuf();
    }

    public void resetExtBuf()
    {
        wrappedBuffer.position(0);
        extRecFirstOffset = -1;
        extRecLastPos = 0; // native layer will start looking from the next commit buffer after the invalid
    }

    public int getExtSize()
    {
        return extSize;
    }

    public void advancedExtRecordLastPos(int newPosition)
    {
        extRecLastPos = newPosition;
    }

    public void updateExtRecordFirstOffset(int value)
    {
        if (extRecFirstOffset == -1) {
            extRecFirstOffset = value;
        }
    }

    public int getExtRecLastPos()
    {
        return extRecLastPos;
    }
}
