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

import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.storage.memory.ThreadArena;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;

public class CollectState
{
    static final StructLayout COLLECT_STATE_LAYOUT;
    private static final long COLLECT_STATE_OFFSET_WARMUP_ELEMENT_PARAMS;
    private static final long COLLECT_STATE_OFFSET_BUFFERS;
    private static final long COLLECT_STATE_OFFSET_RECORD_BUFFER_STATES;
    private static final long COLLECT_STATE_OFFSET_RECORD_INDEXES;
    private static final long COLLECT_STATE_OFFSET_MATCH_COLLECT_METADATA;
    private static final long COLLECT_STATE_OFFSET_FILE_COOKIE;
    private static final long COLLECT_STATE_OFFSET_MIN_FILE_OFFSET;
    private static final long COLLECT_STATE_OFFSET_NUM_RECORDS;
    private static final long COLLECT_STATE_OFFSET_NUM_WARM_UP_ELEMENTS;
    private static final long COLLECT_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS;
    private static final long COLLECT_STATE_OFFSET_NUM_CHUNKS_IN_RANGE;
    private static final long COLLECT_STATE_OFFSET_IS_FULL_SCAN;

    private MemorySegment collectState;
    private MemorySegment collectStateWithPayload;

    static {
        COLLECT_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("pwe_params"),
                ValueLayout.JAVA_LONG.withName("pcollect_buffs"),
                ValueLayout.JAVA_LONG.withName("prec_buf_states"),
                ValueLayout.JAVA_LONG.withName("prec_ixs"),
                ValueLayout.JAVA_LONG.withName("pmatch_collect_infos"),
                RowGroupData.FILE_COOKIE_LAYOUT.withName("file_cookie"),
                ValueLayout.JAVA_INT.withName("min_offset"),
                ValueLayout.JAVA_INT.withName("nrecs"),
                ValueLayout.JAVA_BYTE.withName("nwes"),
                ValueLayout.JAVA_BYTE.withName("nmatch_collect"),
                ValueLayout.JAVA_BYTE.withName("nmultiple_match"),
                ValueLayout.JAVA_BYTE.withName("is_full_scan")).withName("collect_state_t");
        COLLECT_STATE_OFFSET_WARMUP_ELEMENT_PARAMS = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("pwe_params"));
        COLLECT_STATE_OFFSET_BUFFERS = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("pcollect_buffs"));
        COLLECT_STATE_OFFSET_RECORD_BUFFER_STATES = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("prec_buf_states"));
        COLLECT_STATE_OFFSET_RECORD_INDEXES = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("prec_ixs"));
        COLLECT_STATE_OFFSET_MATCH_COLLECT_METADATA = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("pmatch_collect_infos"));
        COLLECT_STATE_OFFSET_FILE_COOKIE = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("file_cookie"));
        COLLECT_STATE_OFFSET_MIN_FILE_OFFSET = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("min_offset"));
        COLLECT_STATE_OFFSET_NUM_RECORDS = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));
        COLLECT_STATE_OFFSET_NUM_WARM_UP_ELEMENTS = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("nwes"));
        COLLECT_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("nmatch_collect"));
        COLLECT_STATE_OFFSET_NUM_CHUNKS_IN_RANGE = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("nmultiple_match"));
        COLLECT_STATE_OFFSET_IS_FULL_SCAN = COLLECT_STATE_LAYOUT.byteOffset(PathElement.groupElement("is_full_scan"));
    }

    public CollectState(
            ThreadArena arena,
            int payloadSize, // payload is taken at the begining of the memory layout
            int storageBufferMetadaSize) // this memory is allocated as a buffer following the match state struct
    {
        this.collectStateWithPayload = arena.allocate(payloadSize + COLLECT_STATE_LAYOUT.byteSize() + storageBufferMetadaSize, ValueLayout.JAVA_LONG.byteSize());
        this.collectState = collectStateWithPayload.asSlice(payloadSize, COLLECT_STATE_LAYOUT);
    }

    public void setState(
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs,
            RecordIndexes recordIndexes,
            int numWesToCollect)
    {
        QueryParams queryParams = queryArgs.queryParams();
        long[] fileCookie = queryArgs.fileCookie();

        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_WARMUP_ELEMENT_PARAMS, aggregatorPageArgs.preLoadedCollectParams().map(m -> m.address()).orElse(0L));
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_BUFFERS, aggregatorPageArgs.collectBuffers().map(m -> m.address()).orElse(0L));
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_RECORD_BUFFER_STATES, aggregatorPageArgs.recordBufferStates().map(m -> m.address()).orElse(0L));
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_RECORD_INDEXES, recordIndexes.getAddress());
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_MATCH_COLLECT_METADATA, aggregatorPageArgs.matchCollectMetadata().map(m -> m.address()).orElse(0L));
        RowGroupData.setFileCookie(
                collectState.asSlice(COLLECT_STATE_OFFSET_FILE_COOKIE, RowGroupData.FILE_COOKIE_LAYOUT),
                (int) fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()],
                fileCookie[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()],
                fileCookie[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()]);
        collectState.set(ValueLayout.JAVA_INT, COLLECT_STATE_OFFSET_MIN_FILE_OFFSET, queryParams.getMinCollectOffset());
        collectState.set(ValueLayout.JAVA_INT, COLLECT_STATE_OFFSET_NUM_RECORDS, queryParams.getTotalNumRecords());
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_NUM_WARM_UP_ELEMENTS, (byte) numWesToCollect);
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS, (byte) queryParams.getNumMatchCollect());
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_NUM_CHUNKS_IN_RANGE, (byte) queryArgs.numChunksInRange());
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_IS_FULL_SCAN, (queryParams.getNumMatchElements() == 0) ? (byte) 1 : (byte) 0);
    }

    public void setLazyState(
            QueryParams queryParams,
            long[] fileCookie,
            int numChunksInRange,
            MemorySegment recordBufferStates,
            RecordIndexes recordIndexes,
            MemorySegment collectBuffers,
            MemorySegment collectParamsMem)
    {
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_WARMUP_ELEMENT_PARAMS, collectParamsMem.address());
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_BUFFERS, collectBuffers.address());
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_RECORD_BUFFER_STATES, recordBufferStates.address());
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_RECORD_INDEXES, recordIndexes.getAddress());
        collectState.set(ValueLayout.JAVA_LONG, COLLECT_STATE_OFFSET_MATCH_COLLECT_METADATA, 0L);
        RowGroupData.setFileCookie(
                collectState.asSlice(COLLECT_STATE_OFFSET_FILE_COOKIE, RowGroupData.FILE_COOKIE_LAYOUT),
                (int) fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()],
                fileCookie[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()],
                fileCookie[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()]);
        collectState.set(ValueLayout.JAVA_INT, COLLECT_STATE_OFFSET_MIN_FILE_OFFSET, queryParams.getMinCollectOffset());
        collectState.set(ValueLayout.JAVA_INT, COLLECT_STATE_OFFSET_NUM_RECORDS, queryParams.getTotalNumRecords());
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_NUM_WARM_UP_ELEMENTS, (byte) 1);
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS, (byte) 0);
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_NUM_CHUNKS_IN_RANGE, (byte) numChunksInRange);
        collectState.set(ValueLayout.JAVA_BYTE, COLLECT_STATE_OFFSET_IS_FULL_SCAN, (queryParams.getNumMatchElements() == 0) ? (byte) 1 : (byte) 0);
    }

    // returns the main memory with the payload
    public MemorySegment getStateMemory()
    {
        return collectStateWithPayload;
    }
}
