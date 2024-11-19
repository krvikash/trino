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

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class ChunkHeader
{
    public static final StructLayout CHUNK_HEADER_LAYOUT;
    //private static final long CHUNK_HEADER_OFFSET_START_LOC;
    //private static final long CHUNK_HEADER_OFFSET_NV;
    //private static final long CHUNK_HEADER_OFFSET_WARM_ID_RELATIVE_START_LOC_TYPE;
    //private static final long CHUNK_HEADER_OFFSET_DATA_INDEX_SPECIFICS;
    private static final long CHUNK_HEADER_OFFSET_MIN;
    //private static final long CHUNK_HEADER_OFFSET_MAX;

    private final MemorySegment chunkHeader;
    private final MemorySegment invalidChunkHeader;

    static {
        CHUNK_HEADER_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("start_loc"),
                ValueLayout.JAVA_INT.withName("nv"),
                ValueLayout.JAVA_INT.withName("warm_id-8b-relative_start_loc-20b-type-4b"),
                ValueLayout.JAVA_INT.withName("data_index_specifics"),
                ValueLayout.JAVA_LONG.withName("min"),
                ValueLayout.JAVA_LONG.withName("max")).withName("chunk_pers_t");
        //CHUNK_HEADER_OFFSET_START_LOC = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("start_loc"));
        //CHUNK_HEADER_OFFSET_NV = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("nv"));
        //CHUNK_HEADER_OFFSET_WARM_ID_RELATIVE_START_LOC_TYPE = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("warm_id-8b-relative_start_loc-20b-type-4b"));
        //CHUNK_HEADER_OFFSET_DATA_INDEX_SPECIFICS = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("data_index_specifics"));
        CHUNK_HEADER_OFFSET_MIN = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("min"));
        //CHUNK_HEADER_OFFSET_MAX = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("max"));
    }

    public ChunkHeader(Arena arena, boolean withAggregates)
    {
        int size = withAggregates ? getHeaderSizeWithAgg() : getHeaderSizeWithoutAgg();
        // one chunk header is used to pass to storage engine to be filled copied back to a list held in java layer
        this.chunkHeader = arena.allocate(size, ValueLayout.JAVA_INT.byteSize());
        // invalid chunk is used for error flows
        this.invalidChunkHeader = MemorySegment.ofArray(new byte[size]);
        this.invalidChunkHeader.fill((byte) -1);
    }

    public long getAddress()
    {
        return chunkHeader.address();
    }

    public int byteSize()
    {
        return (int) chunkHeader.byteSize();
    }

    public MemorySegment getInvalidChunkHeader()
    {
        return invalidChunkHeader;
    }

    public void resetHeader()
    {
        chunkHeader.fill((byte) 0);
    }

    // returns an on heap copy
    public MemorySegment copyChunkHeader()
    {
        final int size = byteSize();
        MemorySegment copyChunkHeader = MemorySegment.ofArray(new byte[size]);
        MemorySegment.copy(chunkHeader, 0, copyChunkHeader, 0, size);
        return copyChunkHeader;
    }

    private static int getHeaderSizeWithAgg()
    {
        return (int) CHUNK_HEADER_LAYOUT.byteSize();
    }

    private static int getHeaderSizeWithoutAgg()
    {
        return (int) CHUNK_HEADER_OFFSET_MIN;
    }
}
