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
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class RangeData
{
    static final StructLayout RECORD_INDEXES_LAYOUT;
    static final SequenceLayout RECORD_INDEXES_LIST_LAYOUT;
    private static final long RECORD_INDEXES_OFFSET_SIZE;
    private static final long RECORD_INDEXES_OFFSET_TYPE;
    private static final long RECORD_INDEXES_OFFSET_START;
    static final long RECORD_INDEXES_OFFSET_LIST; // not private for test

    private final MemorySegment recordIndexes;
    private int numChunkRowsCollected;
    private int lastChunkIndex;
    private final LongArrayList lowerInclusive;
    private final LongArrayList upperExclusive;

    static {
        // since chunk size is not available in static initializer, we will allocate the largest array possible for shorts
        RECORD_INDEXES_LIST_LAYOUT = MemoryLayout.sequenceLayout(1 << Short.SIZE, ValueLayout.JAVA_SHORT);
        RECORD_INDEXES_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("size"),
                ValueLayout.JAVA_SHORT.withName("type"), /* cannot be BYTE since will cause a layout exception for the following short */
                ValueLayout.JAVA_SHORT.withName("start"),
                RECORD_INDEXES_LIST_LAYOUT.withName("list"));
        RECORD_INDEXES_OFFSET_SIZE = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("size"));
        RECORD_INDEXES_OFFSET_TYPE = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("type"));
        RECORD_INDEXES_OFFSET_START = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("start"));
        RECORD_INDEXES_OFFSET_LIST = RECORD_INDEXES_LAYOUT.byteOffset(PathElement.groupElement("list"));
    }

    public RangeData(MemorySegment recordIndexes)
    {
        this.recordIndexes = recordIndexes;
        // ranges are gathered only if needed (mixed query)
        this.numChunkRowsCollected = 0;
        this.lastChunkIndex = 0;
        this.upperExclusive = new LongArrayList();
        this.lowerInclusive = new LongArrayList();
    }

    public int getRecordIndexesSize()
    {
        return recordIndexes.get(ValueLayout.JAVA_INT, RECORD_INDEXES_OFFSET_SIZE);
    }

    public void setRecordIndexesSize(int size)
    {
        recordIndexes.set(ValueLayout.JAVA_INT, RECORD_INDEXES_OFFSET_SIZE, size);
    }

    public RecordIndexListType getRecordIndexesType()
    {
        return RecordIndexListType.values()[recordIndexes.get(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_TYPE)];
    }

    public void setRecordIndexesType(RecordIndexListType type)
    {
        recordIndexes.set(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_TYPE, (short) type.ordinal());
    }

    public short getRecordIndexesStart()
    {
        return recordIndexes.get(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_START);
    }

    public void setRecordIndexesStart(short start)
    {
        recordIndexes.set(ValueLayout.JAVA_SHORT, RECORD_INDEXES_OFFSET_START, start);
    }

    public MemorySegment getRecordIndexesList()
    {
        return recordIndexes.asSlice(RECORD_INDEXES_OFFSET_LIST, RECORD_INDEXES_LIST_LAYOUT);
    }

    public int getRowFromList(MemorySegment recordIndexesList, int listIdx)
    {
        return Short.toUnsignedInt(recordIndexesList.getAtIndex(ValueLayout.JAVA_SHORT, listIdx));
    }

    public int getLastChunkIndex()
    {
        return lastChunkIndex;
    }

    public int getNumChunkRowsCollected()
    {
        return numChunkRowsCollected;
    }

    public void incNumChunkRowsCollected(int numRows)
    {
        numChunkRowsCollected += numRows;
    }

    public void setLastChunkIndex(int chunkIndex)
    {
        lastChunkIndex = chunkIndex;
    }

    public void resetNumChunkRowsCollected()
    {
        numChunkRowsCollected = 0;
    }

    public void addLowerInclusive(long value)
    {
        this.lowerInclusive.add(value);
    }

    public long removeLowerInclusive(int value)
    {
        return this.lowerInclusive.removeLong(value);
    }

    public void removeUpperExclusive(int value)
    {
        this.upperExclusive.removeLong(value);
    }

    public void addUpperExclusive(int value)
    {
        this.upperExclusive.add(value);
    }

    public void clearUpperExclusive()
    {
        this.upperExclusive.clear();
    }

    public void clearLowerInclusive()
    {
        this.lowerInclusive.clear();
    }

    public long[] getLowerInclusiveAsArray()
    {
        return this.lowerInclusive.toLongArray();
    }

    public long[] getUpperExclusiveAsArray()
    {
        return this.upperExclusive.toLongArray();
    }

    public int getLowerInclusiveSize()
    {
        return this.lowerInclusive.size();
    }

    public int getUpperExclusiveSize()
    {
        return this.upperExclusive.size();
    }

    public long getUpperExclusiveValue(int index)
    {
        return this.upperExclusive.getLong(index);
    }
}
