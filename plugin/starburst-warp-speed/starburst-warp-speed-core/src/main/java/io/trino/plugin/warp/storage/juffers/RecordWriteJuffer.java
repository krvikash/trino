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

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_CODE;
import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_LENGTH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX;

public class RecordWriteJuffer
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
    private int recordBufferEntrySize;            // size of one record, one if its a byte buffer
    private boolean isDictionaryValid;

    public RecordWriteJuffer(BufferAllocator bufferAllocator,
            WarmUpElementAllocationParams allocParams,
            StorageEngine storageEngine,
            long weCookie,
            int recTypeCode,
            int recTypeLength,
            int warmUpType,
            long[] fileCookieParams,
            long[] buffAddresses)
    {
        super(bufferAllocator, JuffersType.RECORD);
        this.allocParams = allocParams;
        this.storageEngine = storageEngine;
        this.weCookie = weCookie;
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.warmUpType = warmUpType;
        this.fileCookieParams = fileCookieParams;
        this.buffAddresses = buffAddresses;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        RecTypeCode bufferRecTypeCode;
        int bufferRecTypeLength;
        baseBuffer = bufferAllocator.memorySegment2RecBuff(buffs);
        if (isDictionaryValid) {
            bufferRecTypeCode = DICTIONARY_REC_TYPE_CODE;
            bufferRecTypeLength = DICTIONARY_REC_TYPE_LENGTH;
        }
        else {
            bufferRecTypeCode = allocParams.recTypeCode();
            bufferRecTypeLength = allocParams.recTypeLength();
        }
        this.isDictionaryValid = isDictionaryValid;
        this.wrappedBuffer = createWrapperBuffer(baseBuffer, bufferRecTypeCode, bufferRecTypeLength, true, isDictionaryValid);
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

    public void commitAndResetWE(int numRecs,
            int nullsCount,
            int numBytes,
            long min,
            long max,
            int singleOffset)
    {
        // no need to add to chunk map as we are not closing the chunk

        long res = storageEngine.warmupChunk(weCookie,
                numRecs,
                nullsCount,
                numBytes,
                min,
                max,
                singleOffset,
                false,
                recTypeCode,
                recTypeLength,
                warmUpType,
                fileCookieParams,
                buffAddresses,
                null);
        fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = res & 0xFFFFFFFF;
        fileCookieParams[FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX.ordinal()] = res >> 32;
        resetSingleRecordBufferPos();
    }

    public boolean isDictionaryValid()
    {
        return isDictionaryValid;
    }
}
