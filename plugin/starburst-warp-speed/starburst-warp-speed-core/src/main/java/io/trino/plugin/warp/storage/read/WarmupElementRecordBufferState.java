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
package io.trino.plugin.warp.storage.read;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

class WarmupElementRecordBufferState
{
    static final StructLayout RECORD_BUFFER_STATE_LAYOUT;
    private static final long RECORD_BUFFER_STATE_OFFSET_USED_BYTES;
    private static final long RECORD_BUFFER_STATE_OFFSET_TOTAL_BYTES;
    private static final long RECORD_BUFFER_STATE_OFFSET_MAX_REC_LEN;

    private final MemorySegment recordBufferState;

    static {
        RECORD_BUFFER_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("usedBytes"),
                ValueLayout.JAVA_INT.withName("totalBytes"),
                ValueLayout.JAVA_INT.withName("maxRecordLength")).withName("collect_rec_buf_state_t");
        RECORD_BUFFER_STATE_OFFSET_USED_BYTES = RECORD_BUFFER_STATE_LAYOUT.byteOffset(PathElement.groupElement("usedBytes"));
        RECORD_BUFFER_STATE_OFFSET_TOTAL_BYTES = RECORD_BUFFER_STATE_LAYOUT.byteOffset(PathElement.groupElement("totalBytes"));
        RECORD_BUFFER_STATE_OFFSET_MAX_REC_LEN = RECORD_BUFFER_STATE_LAYOUT.byteOffset(PathElement.groupElement("maxRecordLength"));
    }

    WarmupElementRecordBufferState(MemorySegment recordBufferState, int recordLen)
    {
        recordBufferState.set(ValueLayout.JAVA_INT, RECORD_BUFFER_STATE_OFFSET_MAX_REC_LEN, recordLen);
        this.recordBufferState = recordBufferState;
    }

    public int getUsedBytes()
    {
        return recordBufferState.get(ValueLayout.JAVA_INT, RECORD_BUFFER_STATE_OFFSET_USED_BYTES);
    }

    public int getTotalBytes()
    {
        return recordBufferState.get(ValueLayout.JAVA_INT, RECORD_BUFFER_STATE_OFFSET_TOTAL_BYTES);
    }

    public int getMaxRecordLength()
    {
        return recordBufferState.get(ValueLayout.JAVA_INT, RECORD_BUFFER_STATE_OFFSET_MAX_REC_LEN);
    }

    public int getFreeBytes()
    {
        return getTotalBytes() - getUsedBytes();
    }
}
