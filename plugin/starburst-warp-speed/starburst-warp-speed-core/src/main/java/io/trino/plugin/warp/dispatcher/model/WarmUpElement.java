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
package io.trino.plugin.warp.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.storage.write.WarmupElementStats;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.time.Instant;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@JsonDeserialize(builder = WarmUpElement.Builder.class)
public class WarmUpElement
{
    public static final StructLayout WARM_UP_ELEMENT_ATT_LAYOUT;
    private static final long WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_LENGTH;
    private static final long WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_CODE;
    private static final long WARM_UP_ELEMENT_ATT_OFFSET_WARM_UP_TYPE;

    public static final String WARP_COLUMN = "warpColumn";
    public static final String WARM_UP_TYPE = "warmUpType";
    public static final String REC_TYPE_CODE = "recTypeCode";
    public static final String REC_TYPE_LENGTH = "recTypeLength";
    public static final String WARM_UP_CONTEXT_SIZE = "warmUpContextSize";
    public static final String STATS = "stats";
    public static final String START_OFFSET = "startOffset";
    public static final String QUERY_OFFSET = "queryOffset";
    public static final String QUERY_READ_SIZE = "queryReadSize";
    public static final String WARM_EVENTS = "warmEvents";
    public static final String MATCH_OFFSET = "matchOffset";
    public static final String MATCH_READ_SIZE = "matchReadSize";
    public static final String END_OFFSET = "endOffset";
    public static final String WARM_ID = "warmId";
    public static final String STATE = "state";
    public static final String EXPORT_STATE = "exportState";
    public static final String IS_IMPORTED = "isImported";
    public static final String WARM_STATE = "warmState";
    public static final String FIRST_USED_TIMESTAMP = "firstUsedTimestamp";
    public static final String CREATION_TIME = "creationTime";
    public static final String TOTAL_RECORDS = "total_records";

    private final WarpColumn warpColumn;
    private final WarmUpType warmUpType;
    private final RecTypeCode recTypeCode;
    private final int recTypeLength;
    private final int warmUpContextSize;
    private final WarmupElementStats warmupElementStats;
    private final int startOffset;
    private final int queryOffset;
    private final int queryReadSize;
    private final int warmEvents;
    private final int matchOffset;   // start offset (in pages) of Lucene ChunkState list
    private final int matchReadSize; // size (in pages) of Lucene ChunkState list
    private final int endOffset;
    private final int warmId;
    private final WarmUpElementState state;
    private transient long lastUsedTimestamp;
    private long firstUsedTimestamp;
    private final long creationTime;
    private final ExportState exportState;
    private final boolean isImported;
    private final WarmState warmState;
    private final int totalRecords;

    static {
        WARM_UP_ELEMENT_ATT_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_SHORT.withName("recTypeLength"),
                ValueLayout.JAVA_BYTE.withName("recTypeCode"),
                ValueLayout.JAVA_BYTE.withName("warmUpType")).withName("we_attr_t");
        WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_LENGTH = WARM_UP_ELEMENT_ATT_LAYOUT.byteOffset(PathElement.groupElement("recTypeLength"));
        WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_CODE = WARM_UP_ELEMENT_ATT_LAYOUT.byteOffset(PathElement.groupElement("recTypeCode"));
        WARM_UP_ELEMENT_ATT_OFFSET_WARM_UP_TYPE = WARM_UP_ELEMENT_ATT_LAYOUT.byteOffset(PathElement.groupElement("warmUpType"));
    }

    private WarmUpElement(
            WarpColumn warpColumn,
            WarmUpType warmUpType,
            RecTypeCode recTypeCode,
            int recTypeLength,
            int warmUpContextSize,
            WarmupElementStats warmupElementStats,
            int startOffset,
            int queryOffset,
            int queryReadSize,
            int warmEvents,
            int matchOffset,
            int matchReadSize,
            int endOffset,
            int warmId,
            WarmUpElementState state,
            ExportState exportState,
            boolean isImported,
            WarmState warmState,
            int totalRecords,
            long creationTime,
            long firstUsedTimestamp)
    {
        this.warpColumn = requireNonNull(warpColumn);
        this.warmUpType = requireNonNull(warmUpType);
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.warmUpContextSize = warmUpContextSize;
        this.warmupElementStats = requireNonNull(warmupElementStats);
        this.startOffset = startOffset;
        this.queryOffset = queryOffset;
        this.queryReadSize = queryReadSize;
        this.warmEvents = warmEvents;
        this.matchOffset = matchOffset;
        this.matchReadSize = matchReadSize;
        this.endOffset = endOffset;
        this.warmId = warmId;
        this.state = state;
        this.exportState = exportState;
        this.isImported = isImported;
        this.warmState = warmState;
        this.totalRecords = totalRecords;
        this.creationTime = creationTime;
        this.firstUsedTimestamp = firstUsedTimestamp;
        this.lastUsedTimestamp = System.currentTimeMillis(); // when loading from DB sets to loading time
    }

    public static Builder builder(WarmUpElement warmUpElement)
    {
        return new Builder()
                .warpColumn(warmUpElement.getWarpColumn())
                .warmUpType(warmUpElement.getWarmUpType())
                .recTypeCode(warmUpElement.getRecTypeCode())
                .recTypeLength(warmUpElement.getRecTypeLength())
                .warmUpContextSize(warmUpElement.getWarmUpContextSize())
                .warmupElementStats(warmUpElement.getWarmupElementStats())
                .startOffset(warmUpElement.getStartOffset())
                .queryOffset(warmUpElement.getQueryOffset())
                .queryReadSize(warmUpElement.getQueryReadSize())
                .warmEvents(warmUpElement.getWarmEvents())
                .matchOffset(warmUpElement.getMatchOffset())
                .matchReadSize(warmUpElement.getMatchReadSize())
                .endOffset(warmUpElement.getEndOffset())
                .warmId(warmUpElement.getWarmId())
                .state(warmUpElement.getState())
                .exportState(warmUpElement.getExportState())
                .isImported(warmUpElement.isImported())
                .totalRecords(warmUpElement.getTotalRecords())
                .warmState(warmUpElement.getWarmState())
                .firstUsedTimestamp(warmUpElement.getFirstUsedTimestamp())
                .creationTime(warmUpElement.getCreationTime())
                .lastUsedTimestamp(warmUpElement.getLastUsedTimestamp());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonProperty(WARP_COLUMN)
    public WarpColumn getWarpColumn()
    {
        return warpColumn;
    }

    @JsonProperty(WARM_UP_TYPE)
    public WarmUpType getWarmUpType()
    {
        return warmUpType;
    }

    @JsonProperty(REC_TYPE_CODE)
    public RecTypeCode getRecTypeCode()
    {
        return recTypeCode;
    }

    @JsonProperty(REC_TYPE_LENGTH)
    public int getRecTypeLength()
    {
        return recTypeLength;
    }

    @JsonProperty(WARM_UP_CONTEXT_SIZE)
    public int getWarmUpContextSize()
    {
        return warmUpContextSize;
    }

    @JsonProperty(EXPORT_STATE)
    public ExportState getExportState()
    {
        return exportState;
    }

    @JsonProperty(STATS)
    public WarmupElementStats getWarmupElementStats()
    {
        return warmupElementStats;
    }

    @JsonProperty(STATE)
    public WarmUpElementState getState()
    {
        return state;
    }

    @JsonProperty(IS_IMPORTED)
    public boolean isImported()
    {
        return isImported;
    }

    @JsonProperty(TOTAL_RECORDS)
    public int getTotalRecords()
    {
        return totalRecords;
    }

    @JsonIgnore
    public boolean isValid()
    {
        return WarmUpElementState.State.VALID.equals(state.state());
    }

    @JsonIgnore
    public boolean isRepresentTheSameElement(WarmUpElement other)
    {
        return warpColumn.equals(other.getWarpColumn()) &&
                warmUpType.equals(other.getWarmUpType());
    }

    @JsonIgnore
    public long getLastUsedTimestamp()
    {
        return lastUsedTimestamp;
    }

    @JsonIgnore
    public void setUsedTimestamp(long lastUsedTimestamp)
    {
        if (this.firstUsedTimestamp == 0) {
            this.firstUsedTimestamp = lastUsedTimestamp;
        }
        this.lastUsedTimestamp = lastUsedTimestamp;
    }

    @JsonProperty(START_OFFSET)
    public int getStartOffset()
    {
        return startOffset;
    }

    @JsonProperty(QUERY_OFFSET)
    public int getQueryOffset()
    {
        return queryOffset;
    }

    @JsonProperty(QUERY_READ_SIZE)
    public int getQueryReadSize()
    {
        return queryReadSize;
    }

    @JsonProperty(WARM_EVENTS)
    public int getWarmEvents()
    {
        return warmEvents;
    }

    @JsonProperty(MATCH_OFFSET)
    public int getMatchOffset()
    {
        return matchOffset;
    }

    @JsonProperty(MATCH_READ_SIZE)
    public int getMatchReadSize()
    {
        return matchReadSize;
    }

    @JsonProperty(END_OFFSET)
    public int getEndOffset()
    {
        return endOffset;
    }

    @JsonProperty(WARM_ID)
    public int getWarmId()
    {
        return warmId;
    }

    @JsonProperty(WARM_STATE)
    public WarmState getWarmState()
    {
        return warmState;
    }

    @JsonProperty(FIRST_USED_TIMESTAMP)
    public long getFirstUsedTimestamp()
    {
        return firstUsedTimestamp;
    }

    @JsonProperty(CREATION_TIME)
    public long getCreationTime()
    {
        return creationTime;
    }

    @JsonIgnore
    public boolean isHot()
    {
        return WarmState.HOT.equals(warmState);
    }

    @JsonIgnore
    public static int getRecTypeLength(MemorySegment warmUpElementAtt)
    {
        return Short.toUnsignedInt(warmUpElementAtt.get(ValueLayout.JAVA_SHORT, WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_LENGTH));
    }

    @JsonIgnore
    public static void setRecTypeLength(MemorySegment warmUpElementAtt, int recTypeLength)
    {
        checkArgument(recTypeLength <= 0xFFFF, "recTypeLength %s cannot be converted to unsigned short", recTypeLength);
        warmUpElementAtt.set(ValueLayout.JAVA_SHORT, WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_LENGTH, (short) recTypeLength);
    }

    @JsonIgnore
    public static RecTypeCode getRecTypeCode(MemorySegment warmUpElementAtt)
    {
        return RecTypeCode.values()[warmUpElementAtt.get(ValueLayout.JAVA_BYTE, WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_CODE)];
    }

    @JsonIgnore
    public static void setRecTypeCode(MemorySegment warmUpElementAtt, RecTypeCode recTypeCode)
    {
        warmUpElementAtt.set(ValueLayout.JAVA_BYTE, WARM_UP_ELEMENT_ATT_OFFSET_REC_TYPE_CODE, (byte) recTypeCode.ordinal());
    }

    @JsonIgnore
    public static WarmUpType getWarmUpType(MemorySegment warmUpElementAtt)
    {
        return WarmUpType.values()[warmUpElementAtt.get(ValueLayout.JAVA_BYTE, WARM_UP_ELEMENT_ATT_OFFSET_WARM_UP_TYPE)];
    }

    @JsonIgnore
    public static void setWarmUpType(MemorySegment warmUpElementAtt, WarmUpType warmUpType)
    {
        warmUpElementAtt.set(ValueLayout.JAVA_BYTE, WARM_UP_ELEMENT_ATT_OFFSET_WARM_UP_TYPE, (byte) warmUpType.ordinal());
    }

    @Override
    public String toString()
    {
        return "WarmUpElement{" +
                "warpColumn='" + warpColumn + '\'' +
                ", warmUpType=" + warmUpType +
                ", recTypeCode=" + recTypeCode +
                ", recTypeLength=" + recTypeLength +
                ", warmUpContextSize=" + warmUpContextSize +
                ", exportState=" + exportState +
                ", warmupElementStats=" + warmupElementStats +
                ", state=" + state +
                ", creationTime=" + creationTime +
                ", firstUsedTimestamp=" + firstUsedTimestamp +
                ", lastUsedTimestamp=" + lastUsedTimestamp +
                ", startOffset=" + startOffset +
                ", queryOffset=" + queryOffset +
                ", queryReadSize=" + queryReadSize +
                ", warmEvents=" + warmEvents +
                ", matchOffset=" + matchOffset +
                ", matchReadSize=" + matchReadSize +
                ", endOffset=" + endOffset +
                ", warmId=" + warmId +
                ", warmState=" + warmState +
                ", totalRecords=" + totalRecords +
                ", isImported=" + isImported +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarmUpElement warmUpElement = (WarmUpElement) o;
        return Objects.equals(warpColumn, warmUpElement.warpColumn) &&
                (warmUpType == warmUpElement.warmUpType) &&
                (recTypeCode == warmUpElement.recTypeCode) &&
                (recTypeLength == warmUpElement.recTypeLength) &&
                (warmUpContextSize == warmUpElement.warmUpContextSize) &&
                Objects.equals(warmupElementStats, warmUpElement.warmupElementStats) &&
                (startOffset == warmUpElement.startOffset) &&
                (queryOffset == warmUpElement.queryOffset) &&
                (queryReadSize == warmUpElement.queryReadSize) &&
                (warmEvents == warmUpElement.warmEvents) &&
                (warmId == warmUpElement.warmId) &&
                (totalRecords == warmUpElement.totalRecords) &&
                (matchOffset == warmUpElement.matchOffset) &&
                (matchReadSize == warmUpElement.matchReadSize) &&
                (endOffset == warmUpElement.endOffset) &&
                Objects.equals(state, warmUpElement.state) &&
                Objects.equals(exportState, warmUpElement.exportState) &&
                (isImported == warmUpElement.isImported) &&
                (warmState == warmUpElement.warmState);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(warpColumn, warmUpType, recTypeCode, recTypeLength, warmUpContextSize, warmupElementStats, startOffset, queryOffset, queryReadSize, warmEvents, matchOffset, matchReadSize, endOffset, state, exportState, isImported, warmState, totalRecords);
    }

    @JsonPOJOBuilder
    public static class Builder
    {
        WarpColumn warpColumn;
        WarmUpType warmUpType;
        RecTypeCode recTypeCode;
        int recTypeLength;
        int warmUpContextSize;
        long lastUsedTimestamp = Instant.now().toEpochMilli();
        ExportState exportState = ExportState.NOT_EXPORTED;
        WarmupElementStats warmupElementStats;
        WarmUpElementState state = WarmUpElementState.VALID;
        private int startOffset;
        private int queryOffset;
        private int queryReadSize;
        private int warmEvents;
        private int matchOffset;
        private int matchReadSize;
        private int endOffset;
        private int warmId;
        private long creationTime;
        private long firstUsedTimestamp;

        private int totalRecords;
        private boolean isImported;
        private WarmState warmState = WarmState.HOT;

        @JsonProperty(WARM_UP_TYPE)
        public Builder warmUpType(WarmUpType warmUpType)
        {
            this.warmUpType = warmUpType;
            return this;
        }

        @JsonProperty(REC_TYPE_CODE)
        public Builder recTypeCode(RecTypeCode recTypeCode)
        {
            this.recTypeCode = recTypeCode;
            return this;
        }

        @JsonProperty(REC_TYPE_LENGTH)
        public Builder recTypeLength(int recTypeLength)
        {
            this.recTypeLength = recTypeLength;
            return this;
        }

        @JsonProperty(WARM_UP_CONTEXT_SIZE)
        public Builder warmUpContextSize(int warmUpContextSize)
        {
            this.warmUpContextSize = warmUpContextSize;
            return this;
        }

        @JsonIgnore
        public Builder colName(String colName)
        {
            return warpColumn(new RegularColumn(colName));
        }

        @JsonProperty(WARP_COLUMN)
        public Builder warpColumn(WarpColumn warpColumn)
        {
            this.warpColumn = warpColumn;
            return this;
        }

        @JsonIgnore
        public Builder lastUsedTimestamp(long lastUsedTimestamp)
        {
            this.lastUsedTimestamp = lastUsedTimestamp;
            return this;
        }

        @JsonProperty(EXPORT_STATE)
        public Builder exportState(ExportState exportState)
        {
            this.exportState = exportState;
            return this;
        }

        @JsonProperty(STATE)
        public Builder state(WarmUpElementState state)
        {
            this.state = state;
            return this;
        }

        @JsonProperty(STATS)
        public Builder warmupElementStats(WarmupElementStats warmupElementStats)
        {
            this.warmupElementStats = warmupElementStats;
            return this;
        }

        @JsonProperty(START_OFFSET)
        public Builder startOffset(int startOffset)
        {
            this.startOffset = startOffset;
            return this;
        }

        @JsonProperty(QUERY_OFFSET)
        public Builder queryOffset(int queryOffset)
        {
            this.queryOffset = queryOffset;
            return this;
        }

        @JsonProperty(QUERY_READ_SIZE)
        public Builder queryReadSize(int queryReadSize)
        {
            this.queryReadSize = queryReadSize;
            return this;
        }

        @JsonProperty(WARM_EVENTS)
        public Builder warmEvents(int warmEvents)
        {
            this.warmEvents = warmEvents;
            return this;
        }

        @JsonProperty(MATCH_OFFSET)
        public Builder matchOffset(int matchOffset)
        {
            this.matchOffset = matchOffset;
            return this;
        }

        @JsonProperty(MATCH_READ_SIZE)
        public Builder matchReadSize(int matchReadSize)
        {
            this.matchReadSize = matchReadSize;
            return this;
        }

        @JsonProperty(END_OFFSET)
        public Builder endOffset(int endOffset)
        {
            this.endOffset = endOffset;
            return this;
        }

        @JsonProperty(WARM_ID)
        public Builder warmId(int warmId)
        {
            this.warmId = warmId;
            return this;
        }

        @JsonProperty(IS_IMPORTED)
        public Builder isImported(boolean isImported)
        {
            this.isImported = isImported;
            return this;
        }

        @JsonProperty(WARM_STATE)
        public Builder warmState(WarmState warmState)
        {
            this.warmState = warmState;
            return this;
        }

        @JsonProperty(TOTAL_RECORDS)
        public Builder totalRecords(int totalRecords)
        {
            this.totalRecords = totalRecords;
            return this;
        }

        @JsonProperty(FIRST_USED_TIMESTAMP)
        public Builder firstUsedTimestamp(long firstUsedTimestamp)
        {
            this.firstUsedTimestamp = firstUsedTimestamp;
            return this;
        }

        @JsonProperty(CREATION_TIME)
        public Builder creationTime(long creationTime)
        {
            this.creationTime = creationTime;
            return this;
        }

        public WarmUpElement build()
        {
            WarmUpElement warmUpElement = new WarmUpElement(
                    warpColumn,
                    warmUpType,
                    recTypeCode,
                    recTypeLength,
                    warmUpContextSize,
                    warmupElementStats,
                    startOffset,
                    queryOffset,
                    queryReadSize,
                    warmEvents,
                    matchOffset,
                    matchReadSize,
                    endOffset,
                    warmId,
                    state,
                    exportState,
                    isImported,
                    warmState,
                    totalRecords,
                    creationTime,
                    firstUsedTimestamp);
            warmUpElement.lastUsedTimestamp = lastUsedTimestamp;
            return warmUpElement;
        }
    }
}
