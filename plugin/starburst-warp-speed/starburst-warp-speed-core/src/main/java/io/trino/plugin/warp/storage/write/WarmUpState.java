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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class WarmUpState
{
    // file cookie layout
    private static final StructLayout FILE_COOKIE_LAYOUT;
    private static final long FILE_COOKIE_OFFSET_FILE_HASH;
    private static final long FILE_COOKIE_OFFSET_FILE_MOD_TIME;
    private static final long FILE_COOKIE_OFFSET_FILE_DECSRIPTOR;

    // warm up parameters layout
    public static final StructLayout WARMUP_STATE_LAYOUT;
    private static final long WARMUP_STATE_OFFSET_WRITE_BUF;
    private static final long WARMUP_STATE_OFFSET_FILE_COOKIE;
    public static final long WARMUP_STATE_OFFSET_START_OFFSET; // public for testing
    private static final long WARMUP_STATE_OFFSET_WARM_EVENTS;
    private static final long WARMUP_STATE_OFFSET_ELEMENT_ATT;
    private static final long WARMUP_STATE_OFFSET_NUM_CHUNKS;
    private static final long WARMUP_STATE_OFFSET_WARM_ID;
    private static final long WARMUP_STATE_OFFSET_CLOSE_CHUNK;

    private final MemorySegment warmUpState;

    static {
        FILE_COOKIE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("file_hash"),
                ValueLayout.JAVA_LONG.withName("file_mod_time"),
                ValueLayout.JAVA_INT.withName("file_fd")).withName("storage_file_cookie_t");
        FILE_COOKIE_OFFSET_FILE_HASH = FILE_COOKIE_LAYOUT.byteOffset(PathElement.groupElement("file_hash"));
        FILE_COOKIE_OFFSET_FILE_MOD_TIME = FILE_COOKIE_LAYOUT.byteOffset(PathElement.groupElement("file_mod_time"));
        FILE_COOKIE_OFFSET_FILE_DECSRIPTOR = FILE_COOKIE_LAYOUT.byteOffset(PathElement.groupElement("file_fd"));

        WARMUP_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("pjbuf_ptrs"),
                ValueLayout.JAVA_LONG.withName("pwrite_buf"),
                ValueLayout.JAVA_LONG.withName("pcmprs_stats"),
                ValueLayout.JAVA_LONG.withName("pchunk_pers"),
                FILE_COOKIE_LAYOUT.withName("file_cookie"),
                ValueLayout.JAVA_INT.withName("start_offset"),
                ValueLayout.JAVA_INT.withName("warm_events"),
                WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT.withName("we_attr"),
                ValueLayout.JAVA_SHORT.withName("write_buf_page_ix"),
                ValueLayout.JAVA_SHORT.withName("nchunks"),
                ValueLayout.JAVA_BYTE.withName("warm_id"),
                ValueLayout.JAVA_BYTE.withName("close_chunk")).withName("we_commit_state_t");
        WARMUP_STATE_OFFSET_WRITE_BUF = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("pwrite_buf"));
        WARMUP_STATE_OFFSET_FILE_COOKIE = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("file_cookie"));
        WARMUP_STATE_OFFSET_START_OFFSET = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("start_offset"));
        WARMUP_STATE_OFFSET_WARM_EVENTS = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("warm_events"));
        WARMUP_STATE_OFFSET_ELEMENT_ATT = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("we_attr"));
        WARMUP_STATE_OFFSET_NUM_CHUNKS = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("nchunks"));
        WARMUP_STATE_OFFSET_WARM_ID = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("warm_id"));
        WARMUP_STATE_OFFSET_CLOSE_CHUNK = WARMUP_STATE_LAYOUT.byteOffset(PathElement.groupElement("close_chunk"));
    }

    public WarmUpState(MemorySegment warmUpState)
    {
        this.warmUpState = warmUpState;
    }

    public long getAddress()
    {
        return warmUpState.address();
    }

    public MemorySegment getWarmUpElemetAtt()
    {
        return warmUpState.asSlice(WARMUP_STATE_OFFSET_ELEMENT_ATT, WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT);
    }

    public void setWarmUpElemetAtt(RecTypeCode recTypeCode, int recTypeLength, WarmUpType warmUpType)
    {
        MemorySegment warmUpElementAtt = getWarmUpElemetAtt();
        WarmUpElement.setRecTypeCode(warmUpElementAtt, recTypeCode);
        WarmUpElement.setRecTypeLength(warmUpElementAtt, recTypeLength);
        WarmUpElement.setWarmUpType(warmUpElementAtt, warmUpType);
    }

    public void setFileCookie(int fileDescriptor, long fileHash, long fileModTime)
    {
        MemorySegment fileCookie = warmUpState.asSlice(WARMUP_STATE_OFFSET_FILE_COOKIE, FILE_COOKIE_LAYOUT);
        fileCookie.set(ValueLayout.JAVA_INT, FILE_COOKIE_OFFSET_FILE_DECSRIPTOR, fileDescriptor);
        fileCookie.set(ValueLayout.JAVA_LONG, FILE_COOKIE_OFFSET_FILE_HASH, fileHash);
        fileCookie.set(ValueLayout.JAVA_LONG, FILE_COOKIE_OFFSET_FILE_MOD_TIME, fileModTime);
    }

    public void setWriteBuff(long writeBuffer)
    {
        warmUpState.set(ValueLayout.JAVA_LONG, WARMUP_STATE_OFFSET_WRITE_BUF, writeBuffer);
    }

    public int getStartOffset()
    {
        return warmUpState.get(ValueLayout.JAVA_INT, WARMUP_STATE_OFFSET_START_OFFSET);
    }

    public void setStartOffset(int startOffset)
    {
        warmUpState.set(ValueLayout.JAVA_INT, WARMUP_STATE_OFFSET_START_OFFSET, startOffset);
    }

    public int getWarmEvents()
    {
        return warmUpState.get(ValueLayout.JAVA_INT, WARMUP_STATE_OFFSET_WARM_EVENTS);
    }

    public void resetWarmEvents()
    {
        warmUpState.set(ValueLayout.JAVA_INT, WARMUP_STATE_OFFSET_WARM_EVENTS, 0);
    }

    public byte getWarmId()
    {
        return warmUpState.get(ValueLayout.JAVA_BYTE, WARMUP_STATE_OFFSET_WARM_ID);
    }

    public void setWarmId(byte warmId)
    {
        warmUpState.set(ValueLayout.JAVA_BYTE, WARMUP_STATE_OFFSET_WARM_ID, warmId);
    }

    public void setCloseChunk(boolean closeChunk)
    {
        warmUpState.set(ValueLayout.JAVA_BYTE, WARMUP_STATE_OFFSET_CLOSE_CHUNK, closeChunk ? (byte) 1 : (byte) 0);
    }

    public void setNumChunks(short numChunks)
    {
        warmUpState.set(ValueLayout.JAVA_SHORT, WARMUP_STATE_OFFSET_NUM_CHUNKS, numChunks);
    }
}
