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

import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.plugin.warp.storage.memory.ThreadArena;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

public class RecordIndexes
{
    private static final byte[][] PRECOMPUTED_INDICES;
    private static final long RECORD_INDEX_SIZE;

    static final StructLayout RECORD_INDEXES_LAYOUT;
    static final SequenceLayout RECORD_INDEXES_LIST_LAYOUT;
    private static final long RECORD_INDEXES_OFFSET_SIZE;
    private static final long RECORD_INDEXES_OFFSET_TYPE;
    private static final long RECORD_INDEXES_OFFSET_START;
    static final long RECORD_INDEXES_OFFSET_LIST; // not private for test
    private final int bytesInChunk;

    private MemorySegment recordIndexes;

    static {
        RECORD_INDEX_SIZE = (byte) ValueLayout.JAVA_SHORT.byteSize();
        PRECOMPUTED_INDICES = precomputeIndexes();

        // since chunk size is not available in static initializer, we will allocate the largest array possible for shorts
        RECORD_INDEXES_LIST_LAYOUT = MemoryLayout.sequenceLayout(1 << Short.SIZE, ValueLayout.JAVA_SHORT);
        RECORD_INDEXES_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("size"),
                ValueLayout.JAVA_SHORT.withName("type"), /* cannot be BYTE since will cause a layout exception for the following short */
                ValueLayout.JAVA_SHORT.withName("start"),
                RECORD_INDEXES_LIST_LAYOUT.withName("list")).withName("collect_rec_ixs_t");
        RECORD_INDEXES_OFFSET_SIZE = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("size"));
        RECORD_INDEXES_OFFSET_TYPE = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("type"));
        RECORD_INDEXES_OFFSET_START = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("start"));
        RECORD_INDEXES_OFFSET_LIST = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("list"));
    }

    private static byte[][] precomputeIndexes()
    {
        byte[][] indices = new byte[256][];
        for (int i = 0; i < 256; i++) {
            List<Byte> setBits = new ArrayList<>();
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                if ((i & (1 << bit)) != 0) {
                    setBits.add((byte) bit);
                }
            }
            // Convert List<Byte> to byte[] for efficiency
            byte[] byteArray = new byte[setBits.size()];
            for (int j = 0; j < setBits.size(); j++) {
                byteArray[j] = setBits.get(j);
            }
            indices[i] = byteArray;
        }
        return indices;
    }

    // CTOR for supporting a full indexes list
    public RecordIndexes(ThreadArena arena, int chunkSize)
    {
        this.bytesInChunk = chunkSize / Byte.SIZE;
        this.recordIndexes = arena.allocate(RECORD_INDEXES_LAYOUT.byteSize(), ValueLayout.JAVA_SHORT.byteSize());
    }

    // CTOR for full scan case where only the header is required
    public RecordIndexes(SegmentAllocator allocator)
    {
        this.bytesInChunk = 0;
        this.recordIndexes = allocator.allocate(RECORD_INDEXES_OFFSET_LIST, ValueLayout.JAVA_SHORT.byteSize());
    }

    public MemorySegment getMemory()
    {
        return recordIndexes;
    }

    public long getAddress()
    {
        return recordIndexes.address();
    }

    public int getSize()
    {
        return recordIndexes.get(ValueLayout.JAVA_INT, RECORD_INDEXES_OFFSET_SIZE);
    }

    public void setSize(int size)
    {
        recordIndexes.set(ValueLayout.JAVA_INT, RECORD_INDEXES_OFFSET_SIZE, size);
    }

    public RecordIndexListType getType()
    {
        return RecordIndexListType.values()[recordIndexes.get(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_TYPE)];
    }

    public void setType(RecordIndexListType type)
    {
        recordIndexes.set(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_TYPE, (short) type.ordinal());
    }

    public int getStart()
    {
        return Short.toUnsignedInt(recordIndexes.get(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_START));
    }

    public void setStart(int start)
    {
        recordIndexes.set(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_START, (short) start);
    }

    public MemorySegment getList()
    {
        if (bytesInChunk == 0) {
            throw new RuntimeException("record indexes list requested on full scan case");
        }
        return recordIndexes.asSlice(RECORD_INDEXES_OFFSET_LIST, RECORD_INDEXES_LIST_LAYOUT);
    }

    public int getRowFromList(MemorySegment recordIndexesList, int listIdx)
    {
        return Short.toUnsignedInt(recordIndexesList.getAtIndex(ValueLayout.JAVA_SHORT, listIdx));
    }

    public int setRecIxListFromBM(MemorySegment bm)
    {
        int numRecords = 0;

        for (long byteIndex = 0; byteIndex < bytesInChunk; byteIndex++) {
            int byteValue = Byte.toUnsignedInt(bm.get(ValueLayout.JAVA_BYTE, byteIndex));
            for (int bitOffset : PRECOMPUTED_INDICES[byteValue]) {
                long recIxOffset = RECORD_INDEXES_OFFSET_LIST + numRecords * RECORD_INDEX_SIZE;
                recordIndexes.set(ValueLayout.JAVA_SHORT, recIxOffset,
                        (short) (byteIndex * Byte.SIZE + bitOffset));
                numRecords++;
            }
        }
        return numRecords;
    }

    public void reset(int size, RecordIndexListType type)
    {
        setSize(size);
        setType(type);
        setStart(0);
    }
}
