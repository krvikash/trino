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

import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.block.Block;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Optional;

public class WarmupElementCollectParams
{
    static final StructLayout WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT;
    private static final long WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_OFFSET;
    private static final long WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT;
    private static final long WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_READ_SIZE;
    private static final long WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_MATCH_COLLECT_INDEX;
    private static final long WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_IS_COLLECT_NULLS;
    private static final long WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_ID;

    private final MemorySegment collectParamsMem;
    // the record type code and length for the page block returned to trino
    private final RecTypeCode blockRecTypeCode;
    private final int blockRecTypeLength;
    private final int warmEvents;
    private final boolean isImported;
    private final int blockIndex;
    private final Optional<Block> valuesDictBlock;

    static {
        WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("entry_loc"),
                WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT.withName("we_attr"),
                ValueLayout.JAVA_SHORT.withName("read_size"),
                ValueLayout.JAVA_BYTE.withName("match_collect_ix"),
                ValueLayout.JAVA_BYTE.withName("collect_nulls"),
                ValueLayout.JAVA_BYTE.withName("warm_id"),
                ValueLayout.JAVA_BYTE.withName("padding"),
                ValueLayout.JAVA_BYTE.withName("padding"),
                ValueLayout.JAVA_BYTE.withName("padding")).withName("we_collect_params_t");
        WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_OFFSET = WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("entry_loc"));
        WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT = WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("we_attr"));
        WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_READ_SIZE = WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("read_size"));
        WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_MATCH_COLLECT_INDEX = WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("match_collect_ix"));
        WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_IS_COLLECT_NULLS = WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("collect_nulls"));
        WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_ID = WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("warm_id"));
    }

    public WarmupElementCollectParams(
            MemorySegment collectParamsMem,
            int fileOffset,
            RecTypeCode recTypeCode,
            int recTypeLength,
            WarmUpType warmUpType,
            int fileReadSize,
            int matchCollectIndex,
            boolean isCollectNulls,
            int warmId,
            RecTypeCode blockRecTypeCode,
            int blockRecTypeLength,
            int warmEvents,
            boolean isImported,
            int blockIndex,
            Optional<Block> valuesDictBlock)
    {
        this.collectParamsMem = collectParamsMem;
        collectParamsMem.set(ValueLayout.JAVA_INT, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_OFFSET, fileOffset);
        collectParamsMem.set(ValueLayout.JAVA_SHORT, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_READ_SIZE, (short) fileReadSize);
        collectParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_MATCH_COLLECT_INDEX, (byte) matchCollectIndex);
        collectParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_IS_COLLECT_NULLS, isCollectNulls ? (byte) 1 : (byte) 0);
        collectParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_ID, (byte) warmId);

        if (recTypeLength < 0) {
            throw new RuntimeException("record length is negative");
        }
        MemorySegment warmupElementAtt = getWarmupElementAtt();
        WarmUpElement.setRecTypeCode(warmupElementAtt, TypeUtils.nativeRecTypeCode(recTypeCode));
        WarmUpElement.setRecTypeLength(warmupElementAtt, recTypeLength);
        WarmUpElement.setWarmUpType(warmupElementAtt, warmUpType);

        this.blockRecTypeCode = blockRecTypeCode;
        this.blockRecTypeLength = blockRecTypeLength;
        this.warmEvents = warmEvents;
        this.isImported = isImported;
        this.blockIndex = blockIndex;
        this.valuesDictBlock = valuesDictBlock;
    }

    public MemorySegment getMemory()
    {
        return collectParamsMem;
    }

    public MemorySegment getWarmupElementAtt()
    {
        return collectParamsMem.asSlice(WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT, WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT);
    }

    public boolean isCollectNulls()
    {
        return collectParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_IS_COLLECT_NULLS) != 0;
    }

    public boolean hasMatchCollect()
    {
        return collectParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_MATCH_COLLECT_INDEX) != -1;
    }

    public RecTypeCode getBlockRecTypeCode()
    {
        return blockRecTypeCode;
    }

    public int getBlockRecTypeLength()
    {
        return blockRecTypeLength;
    }

    public boolean mappedMatchCollect()
    {
        return valuesDictBlock.isPresent();
    }

    public Optional<Block> getValuesDictBlock()
    {
        return valuesDictBlock;
    }

    public int getBlockIndex()
    {
        return blockIndex;
    }

    @Override
    public int hashCode()
    {
        MemorySegment warmupElementAtt = getWarmupElementAtt();
        return Objects.hash(WarmUpElement.getRecTypeCode(warmupElementAtt),
                WarmUpElement.getRecTypeLength(warmupElementAtt),
                WarmUpElement.getWarmUpType(warmupElementAtt),
                getFileOffset());
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof WarmupElementCollectParams o)) {
            return false;
        }

        MemorySegment warmupElementAtt = getWarmupElementAtt();
        MemorySegment otherWarmupElementAtt = o.getWarmupElementAtt();
        return getFileOffset() == o.getFileOffset() &&
                WarmUpElement.getRecTypeCode(warmupElementAtt) == WarmUpElement.getRecTypeCode(otherWarmupElementAtt) &&
                WarmUpElement.getRecTypeLength(warmupElementAtt) == WarmUpElement.getRecTypeLength(otherWarmupElementAtt) &&
                WarmUpElement.getWarmUpType(warmupElementAtt) == WarmUpElement.getWarmUpType(otherWarmupElementAtt);
    }

    @Override
    public String toString()
    {
        MemorySegment warmupElementAtt = getWarmupElementAtt();
        WarmUpType warmUpType = WarmUpElement.getWarmUpType(warmupElementAtt);
        return "WarmupElementCollectParams{" +
                "fileOffset=" + collectParamsMem.get(ValueLayout.JAVA_INT, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_OFFSET) +
                ", recTypeCode=" + WarmUpElement.getRecTypeCode(warmupElementAtt) +
                ", recTypeLength=" + WarmUpElement.getRecTypeLength(warmupElementAtt) +
                ", warmUpType=" + warmUpType +
                ", fileReadSize=" + collectParamsMem.get(ValueLayout.JAVA_SHORT, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_READ_SIZE) +
                ", matchCollectIndex=" + collectParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_MATCH_COLLECT_INDEX) +
                ", isCollectNulls=" + collectParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_IS_COLLECT_NULLS) +
                ", warmId=" + collectParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_WARM_ID) +
                ", blockRecTypeCode=" + blockRecTypeCode +
                ", blockRecTypeLength=" + blockRecTypeLength +
                ", warmEvents=" + warmEvents +
                ", isImported=" + isImported +
                ", blockIndex=" + blockIndex +
                '}';
    }

    private int getFileOffset()
    {
        return collectParamsMem.get(ValueLayout.JAVA_INT, WARMUP_ELEMENT_COLLECT_PARAMS_OFFSET_FILE_OFFSET);
    }
}
