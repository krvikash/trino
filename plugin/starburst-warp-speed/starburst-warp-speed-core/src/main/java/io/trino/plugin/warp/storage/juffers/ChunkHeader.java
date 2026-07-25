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

import com.google.common.annotations.VisibleForTesting;
import io.trino.plugin.warp.gen.constants.StorageLocHomogeneous;
import io.trino.plugin.warp.storage.memory.ThreadArena;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class ChunkHeader
{
    public static final int MAX_RELATIVE_OFFSET = 1 << 24;
    public static final StructLayout CHUNK_HEADER_LAYOUT;
    private static final long CHUNK_HEADER_OFFSET_START_LOC;
    private static final long CHUNK_HEADER_OFFSET_NV;
    private static final long CHUNK_HEADER_OFFSET_RELATIVE_START_LOC_LOW;
    private static final long CHUNK_HEADER_OFFSET_RELATIVE_START_LOC_HIGH;
    public static final long CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID; // for testing
    private static final long CHUNK_HEADER_OFFSET_MIN;

    private final MemorySegment chunkHeader;

    static {
        CHUNK_HEADER_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("start_loc"),
                ValueLayout.JAVA_INT.withName("nv"),
                ValueLayout.JAVA_SHORT.withName("relative_start_loc_low"),
                ValueLayout.JAVA_BYTE.withName("relative_start_loc_high"),
                ValueLayout.JAVA_BYTE.withName("type_and_warm_id"), // type is low 4 bits and warm id is high 4 bits
                ValueLayout.JAVA_INT.withName("data_index_specifics"),
                ValueLayout.JAVA_LONG.withName("min"),
                ValueLayout.JAVA_LONG.withName("max")).withName("chunk_pers_t");
        CHUNK_HEADER_OFFSET_START_LOC = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("start_loc"));
        CHUNK_HEADER_OFFSET_NV = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("nv"));
        CHUNK_HEADER_OFFSET_RELATIVE_START_LOC_LOW = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("relative_start_loc_low"));
        CHUNK_HEADER_OFFSET_RELATIVE_START_LOC_HIGH = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("relative_start_loc_high"));
        CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("type_and_warm_id"));
        CHUNK_HEADER_OFFSET_MIN = CHUNK_HEADER_LAYOUT.byteOffset(PathElement.groupElement("min"));
    }

    public ChunkHeader(ThreadArena arena, boolean withAggregates)
    {
        int size = withAggregates ? getHeaderSizeWithAgg() : getHeaderSizeWithoutAgg();
        // one chunk header is used to pass to storage engine to be filled copied back to a list held in java layer
        this.chunkHeader = arena.allocate(size, ValueLayout.JAVA_INT.byteSize());
    }

    public long getAddress()
    {
        return chunkHeader.address();
    }

    public int byteSize()
    {
        return (int) chunkHeader.byteSize();
    }

    public void resetHeader(byte warmId)
    {
        chunkHeader.fill((byte) 0);
        chunkHeader.set(ValueLayout.JAVA_BYTE, CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID, (byte) (warmId << 4));
    }

    // verify chunk header properties and returns an on heap copy
    public MemorySegment verifyAndCopyChunkHeader()
    {
        if (!verifyStartOffset() || !verifyNullsOffset() || !isValid()) {
            throw new RuntimeException("chunk header is invalid startOffset " + getStartOffset() + " nullsOffset " + getNullsOffset() + " typeAndWarmId " + getTypeAndWarmId());
        }

        final int size = byteSize();
        MemorySegment copyChunkHeader = MemorySegment.ofArray(new byte[size]);
        MemorySegment.copy(chunkHeader, 0, copyChunkHeader, 0, size);
        return copyChunkHeader;
    }

    public boolean isValid()
    {
        return (getTypeAndWarmId() & 0xF) != 0;
    }

    private boolean verifyStartOffset()
    {
        int startOffset = getStartOffset();
        return (startOffset >= 0);
    }

    private boolean verifyNullsOffset()
    {
        int nullsOffset = getNullsOffset();
        return (nullsOffset >= 0) ||
                (nullsOffset == -1 * StorageLocHomogeneous.STORAGE_LOC_HOMOGENEOUS_ONE.ordinal()) ||
                (nullsOffset == -1 * StorageLocHomogeneous.STORAGE_LOC_HOMOGENEOUS_ZERO.ordinal());
    }

    private int getStartOffset()
    {
        return (int) chunkHeader.get(ValueLayout.JAVA_INT, CHUNK_HEADER_OFFSET_START_LOC);
    }

    private int getNullsOffset()
    {
        return (int) chunkHeader.get(ValueLayout.JAVA_INT, CHUNK_HEADER_OFFSET_NV);
    }

    public static void finalizeChunkHeader(MemorySegment copyChunkHeader, MemorySegment chunkHeader, int baseOffset)
    {
        MemorySegment.copy(copyChunkHeader, 0, chunkHeader, 0, copyChunkHeader.byteSize());

        final int startOffset = chunkHeader.get(ValueLayout.JAVA_INT, CHUNK_HEADER_OFFSET_START_LOC);
        final int relativeOffset = baseOffset - startOffset;
        // check validity
        if ((startOffset < 0) || (relativeOffset < 0) || (relativeOffset > MAX_RELATIVE_OFFSET)) {
            throw new RuntimeException("invalid chunk header offsets: startOffset " + startOffset + " baseOffset " + baseOffset + " relativeOffset " + relativeOffset);
        }

        // set relative offset and then set start location to invalid
        chunkHeader.set(ValueLayout.JAVA_SHORT, CHUNK_HEADER_OFFSET_RELATIVE_START_LOC_LOW, (short) (relativeOffset & 0xFFFF));
        chunkHeader.set(ValueLayout.JAVA_BYTE, CHUNK_HEADER_OFFSET_RELATIVE_START_LOC_HIGH, (byte) (relativeOffset >> 16));
        chunkHeader.set(ValueLayout.JAVA_INT, CHUNK_HEADER_OFFSET_START_LOC, -1 * StorageLocHomogeneous.STORAGE_LOC_HOMOGENEOUS_INVALID.ordinal());
    }

    private byte getTypeAndWarmId()
    {
        return (byte) chunkHeader.get(ValueLayout.JAVA_BYTE, CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID);
    }

    private static int getHeaderSizeWithAgg()
    {
        return (int) CHUNK_HEADER_LAYOUT.byteSize();
    }

    private static int getHeaderSizeWithoutAgg()
    {
        return (int) CHUNK_HEADER_OFFSET_MIN;
    }

    @VisibleForTesting
    void setTypeAndWarmId(byte typeAndWarmId)
    {
        chunkHeader.set(ValueLayout.JAVA_BYTE, CHUNK_HEADER_OFFSET_TYPE_AND_WARM_ID, (byte) typeAndWarmId);
    }
}
