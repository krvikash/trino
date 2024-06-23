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

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX;

/**
 * buffer for marking extended strings
 */
public class ExtendedJuffer
        extends BaseWriteJuffer
{
    private final WarmUpElementAllocationParams allocParams;
    private final StorageEngine storageEngine;
    private final long weCookie;
    private final int recTypeCode;
    private final int recTypeLength;
    private final int warmUpType;
    private final long[] fileCookieParams;
    private final long[] buffAddresses;
    private int extWESize;                          // extended recs buffer size
    private int extRecordFirstOffset;               // offset of the first extended entry we encountered
    private int extRecordLastPos;                   // extended records last entry address

    public ExtendedJuffer(BufferAllocator bufferAllocator,
            WarmUpElementAllocationParams allocParams,
            StorageEngine storageEngine,
            long weCookie,
            int recTypeCode,
            int recTypeLength,
            int warmUpType,
            long[] fileCookieParams,
            long[] buffAddresses)
    {
        super(bufferAllocator, JuffersType.EXTENDED_REC);
        this.allocParams = allocParams;
        this.storageEngine = storageEngine;
        this.weCookie = weCookie;
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.warmUpType = warmUpType;
        this.fileCookieParams = fileCookieParams;
        this.buffAddresses = buffAddresses;
        extRecordFirstOffset = -1;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.memorySegment2ExtRecsBuff(buffs));
        this.wrappedBuffer = this.baseBuffer;
        this.extWESize = allocParams.extRecBuffSize();
    }

    protected void commitAndResetExtRecordBuffer(int numExtBytes)
    {
        if (numExtBytes > 0) {
            long res = storageEngine.warmupChunkExtRec(weCookie,
                    extRecordFirstOffset,
                    numExtBytes,
                    recTypeCode,
                    recTypeLength,
                    warmUpType,
                    fileCookieParams,
                    buffAddresses);
            fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = res & 0xFFFFFFFF;
            fileCookieParams[FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX.ordinal()] = res >> 32;
            resetExtBuf();
        }
    }

    public void commitAndResetExtRecordBuffer()
    {
        if (wrappedBuffer.position() > 0) {
            long res = storageEngine.warmupChunkExtRec(weCookie,
                    extRecordFirstOffset,
                    wrappedBuffer.position(),
                    recTypeCode,
                    recTypeLength,
                    warmUpType,
                    fileCookieParams,
                    buffAddresses);
            fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = res & 0xFFFFFFFF;
            fileCookieParams[FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX.ordinal()] = res >> 32;
        }
        resetExtBuf();
    }

    public void resetExtBuf()
    {
        wrappedBuffer.position(0);
        extRecordFirstOffset = -1;
        extRecordLastPos = 0; // native layer will start looking from the next commit buffer after the invalid
    }

    public int getExtWESize()
    {
        return extWESize;
    }

    public void advancedExtRecordLastPos(int newPosition)
    {
        extRecordLastPos = newPosition;
    }

    public void updateExtRecordFirstOffset(int value)
    {
        if (extRecordFirstOffset == -1) {
            extRecordFirstOffset = value;
        }
    }

    public int getExtRecordLastPos()
    {
        return extRecordLastPos;
    }
}
