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
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.gen.constants.BasicWarmEvents;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.MatchNodeType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.type.TypeUtils;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator.INVALID_WARM_ID;

public class WarmupElementMatchParams
        implements MatchNode
{
    static final StructLayout WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_PREDICATE_BUFFER_ADDRESS;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_OFFSET;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_READ_SIZE;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_OP;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_INDEX;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_ID;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_COLLECT_NULLS;
    private static final long WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_TIGHTNESS_REQUIRED;

    private final MemorySegment matchParamsMem;
    private final int warmEvents;
    private final boolean isImported;
    private final Optional<WarmupElementLuceneParams> luceneParams;
    private final MatchNodeAtt matchNodeAtt;

    static {
        WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("ppre_collect_bufs"),
                ValueLayout.JAVA_LONG.withName("ppred_buf"),
                ValueLayout.JAVA_INT.withName("entry_loc"),
                WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT.withName("we_attr"),
                ValueLayout.JAVA_SHORT.withName("read_size"),
                ValueLayout.JAVA_BYTE.withName("match_collect_op"),
                ValueLayout.JAVA_BYTE.withName("match_collect_ix"),
                ValueLayout.JAVA_BYTE.withName("warm_id"),
                ValueLayout.JAVA_BYTE.withName("collect_nulls"),
                ValueLayout.JAVA_BYTE.withName("tightness_required"),
                ValueLayout.JAVA_BYTE.withName("padding")).withName("we_match_params_t");
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_PREDICATE_BUFFER_ADDRESS = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("ppred_buf"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_OFFSET = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("entry_loc"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("we_attr"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_READ_SIZE = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("read_size"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_OP = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("match_collect_op"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_INDEX = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("match_collect_ix"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_ID = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("warm_id"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_COLLECT_NULLS = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("collect_nulls"));
        WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_TIGHTNESS_REQUIRED = WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("tightness_required"));
    }

    public WarmupElementMatchParams(
            MemorySegment matchParamsMem,
            MemorySegment predBuf,
            int fileOffset,
            RecTypeCode recTypeCode,
            int recTypeLength,
            WarmUpType warmUpType,
            int fileReadSize,
            MatchCollectOp matchCollectOp,
            int matchCollectIndex,
            boolean isCollectNulls,
            boolean isTightnessRequired,
            int warmEvents,
            boolean isImported,
            Optional<WarmupElementLuceneParams> luceneParams,
            MemorySegment matchNodeAttMem,
            int leafIx)
    {
        this.matchParamsMem = matchParamsMem;
        matchParamsMem.set(ValueLayout.JAVA_LONG, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_PREDICATE_BUFFER_ADDRESS, predBuf.address());
        matchParamsMem.set(ValueLayout.JAVA_INT, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_OFFSET, fileOffset);
        matchParamsMem.set(ValueLayout.JAVA_SHORT, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_READ_SIZE, (short) fileReadSize);
        matchParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_OP, (byte) matchCollectOp.ordinal());
        matchParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_INDEX, (byte) matchCollectIndex);
        matchParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_ID, (byte) INVALID_WARM_ID);
        matchParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_COLLECT_NULLS, isCollectNulls ? (byte) 1 : (byte) 0);
        matchParamsMem.set(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_TIGHTNESS_REQUIRED, isTightnessRequired ? (byte) 1 : (byte) 0);

        MemorySegment warmupElementAtt = matchParamsMem.asSlice(WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT, WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT);
        WarmUpElement.setRecTypeCode(warmupElementAtt, TypeUtils.nativeRecTypeCode(recTypeCode));
        WarmUpElement.setRecTypeLength(warmupElementAtt, recTypeLength);
        WarmUpElement.setWarmUpType(warmupElementAtt, warmUpType);

        this.warmEvents = warmEvents;
        this.isImported = isImported;
        this.luceneParams = luceneParams;

        this.matchNodeAtt = new MatchNodeAtt(matchNodeAttMem, getNodeType(), leafIx);
    }

    public boolean hasLuceneParams()
    {
        return luceneParams.isPresent();
    }

    public LuceneQueryMatchData getLuceneQueryMatchData()
    {
        return luceneParams.get().luceneQueryMatchData();
    }

    public int getLuceneIx()
    {
        return luceneParams.get().luceneIx();
    }

    @Override
    public MatchNodeType getNodeType()
    {
        return MatchNodeType.MATCH_NODE_TYPE_LEAF;
    }

    @Override
    public List<MatchNode> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public int getSubtreeSize()
    {
        return 1;
    }

    @Override
    public int getHeight()
    {
        return 1;
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
        if (!(other instanceof WarmupElementMatchParams o)) {
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
        return "WarmupElementMatchParams{" +
                "fileOffset=" + matchParamsMem.get(ValueLayout.JAVA_INT, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_OFFSET) +
                ", recTypeCode=" + WarmUpElement.getRecTypeCode(warmupElementAtt) +
                ", recTypeLength=" + WarmUpElement.getRecTypeLength(warmupElementAtt) +
                ", warmUpType=" + WarmUpElement.getWarmUpType(warmupElementAtt) +
                ", predicateBufferAddress=" + matchParamsMem.get(ValueLayout.JAVA_LONG, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_PREDICATE_BUFFER_ADDRESS) +
                ", fileReadSize=" + matchParamsMem.get(ValueLayout.JAVA_SHORT, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_READ_SIZE) +
                ", matchCollectOp=" + matchParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_OP) +
                ", matchCollectIndex=" + matchParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_MATCH_COLLECT_INDEX) +
                ", isCollectNulls=" + matchParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_COLLECT_NULLS) +
                ", isTightnessRequired=" + matchParamsMem.get(ValueLayout.JAVA_BYTE, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_IS_TIGHTNESS_REQUIRED) +
                ", warmEvents=" + warmEventsToString() +
                ", isImported=" + isImported +
                ", luceneParams=" + luceneParams +
                ", matchNodeAtt=" + matchNodeAtt +
                '}';
    }

    private int getFileOffset()
    {
        return matchParamsMem.get(ValueLayout.JAVA_INT, WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_FILE_OFFSET);
    }

    private MemorySegment getWarmupElementAtt()
    {
        return matchParamsMem.asSlice(WARMUP_ELEMENT_MATCH_PARAMS_OFFSET_WARM_UP_ELEMENT_ATT, WarmUpElement.WARM_UP_ELEMENT_ATT_LAYOUT);
    }

    private String warmEventsToString()
    {
        if (WarmUpElement.getWarmUpType(getWarmupElementAtt()) == WarmUpType.WARM_UP_TYPE_BASIC) {
            return "basic" +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_COMPRESSION_LZ.ordinal()) ? ":compression_lz" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_DIRECT_ROOT.ordinal()) ? ":direct_root" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_RAW.ordinal()) ? ":entry_raw" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_RANGE.ordinal()) ? ":entry_range" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_DELTA.ordinal()) ? ":entry_delta" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_BM.ordinal()) ? ":entry_bm" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_DELTA_BIT_PACKING.ordinal()) ? ":entry_delta_bit_packing" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_SINGLES.ordinal()) ? ":entry_singles" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_SINGLE_CHUNK.ordinal()) ? ":single_chunk" : "");
        }
        return "no events";
    }

    private boolean eventOccurred(int eventNum)
    {
        return (warmEvents & (1 << eventNum)) != 0;
    }
}
