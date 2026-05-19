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

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class RecordBufferParams
{
    public static final StructLayout RECORD_BUFFER_PARAMS_LAYOUT;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_MIN;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_MAX;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_NRECS;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_NVS;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_SIZE;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_SINGLE_VAL_OFFSET;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_EXT_SIZE;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_EXT_REC_FIRST_OFFSET;

    private final MemorySegment recordBufferParams;

    static {
        RECORD_BUFFER_PARAMS_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("min"),
                ValueLayout.JAVA_LONG.withName("max"),
                ValueLayout.JAVA_INT.withName("nrecs"),
                ValueLayout.JAVA_INT.withName("nvs"),
                ValueLayout.JAVA_INT.withName("size"),
                ValueLayout.JAVA_INT.withName("singleValOffset"),
                ValueLayout.JAVA_INT.withName("extSize"),
                ValueLayout.JAVA_INT.withName("extRecFirstOffset")).withName("rec_buf_t");
        RECORD_BUFFER_PARAMS_OFFSET_MIN = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("min"));
        RECORD_BUFFER_PARAMS_OFFSET_MAX = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("max"));
        RECORD_BUFFER_PARAMS_OFFSET_NRECS = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));
        RECORD_BUFFER_PARAMS_OFFSET_NVS = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("nvs"));
        RECORD_BUFFER_PARAMS_OFFSET_SIZE = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("size"));
        RECORD_BUFFER_PARAMS_OFFSET_SINGLE_VAL_OFFSET = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("singleValOffset"));
        RECORD_BUFFER_PARAMS_OFFSET_EXT_SIZE = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("extSize"));
        RECORD_BUFFER_PARAMS_OFFSET_EXT_REC_FIRST_OFFSET = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("extRecFirstOffset"));
    }

    public RecordBufferParams(MemorySegment recordBufferParams)
    {
        this.recordBufferParams = recordBufferParams;
    }

    public MemorySegment getMemory()
    {
        return recordBufferParams;
    }

    public void setParams(
            long min,
            long max,
            int numRecords,
            int numNulls,
            int numBytes,
            int singleValOffset)
    {
        if (numRecords <= 0) {
            throw new RuntimeException("warmup chunk called with illegal number of rows " + numRecords);
        }

        recordBufferParams.set(ValueLayout.JAVA_LONG, RECORD_BUFFER_PARAMS_OFFSET_MIN, min);
        recordBufferParams.set(ValueLayout.JAVA_LONG, RECORD_BUFFER_PARAMS_OFFSET_MAX, max);
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_NRECS, numRecords);
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_NVS, numNulls);
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_SIZE, numBytes);
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_SINGLE_VAL_OFFSET, singleValOffset);
    }

    // set paramters for extended records write
    public void setExtParams(int numExtBytes, int extRecFirstOffset)
    {
        if (numExtBytes <= 0) {
            throw new RuntimeException("warmup chunk extended records called with illegal number of bytes " + numExtBytes);
        }

        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_EXT_SIZE, numExtBytes);
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_EXT_REC_FIRST_OFFSET, extRecFirstOffset);
    }
}
