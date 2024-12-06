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

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;

public class MatchState
{
    private static final StructLayout MATCH_LUCENE_STATE_LAYOUT;
    private static final long MATCH_LUCENE_STATE_OFFSET_UNIQUE_ID;
    private static final long MATCH_LUCENE_STATE_OFFSET_NUM_RECORDS;

    static final StructLayout MATCH_STATE_LAYOUT;
    private static final long MATCH_STATE_OFFSET_MATCH_TREE;
    private static final long MATCH_STATE_OFFSET_WARMUP_ELEMENT_PARAMS;
    private static final long MATCH_STATE_OFFSET_MATCH_BM;
    private static final long MATCH_STATE_OFFSET_LUCENE_BM;
    private static final long MATCH_STATE_OFFSET_MATCH_COLLECT_MD;
    private static final long MATCH_STATE_OFFSET_FILE_COOKIE;
    private static final long MATCH_STATE_OFFSET_LUCENE_STATE;
    private static final long MATCH_STATE_OFFSET_NUM_RECORDS;
    private static final long MATCH_STATE_OFFSET_MIN_FILE_OFFSET;
    private static final long MATCH_STATE_OFFSET_MATCH_COLLECT_ID;
    private static final long MATCH_STATE_OFFSET_TX_ID;
    private static final long MATCH_STATE_OFFSET_NUM_WARM_UP_ELEMENTS;
    private static final long MATCH_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS;
    private static final long MATCH_STATE_OFFSET_NUM_CHUNKS_IN_RANGE;
    private static final long MATCH_STATE_OFFSET_MAX_TREE_HEIGHT;

    private final MemorySegment matchState;
    private final MemorySegment matchStateWithPayload;

    static {
        MATCH_LUCENE_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("unique_id"),
                ValueLayout.JAVA_INT.withName("nrecs")).withName("match_lucene_state_t");
        MATCH_LUCENE_STATE_OFFSET_UNIQUE_ID = MATCH_LUCENE_STATE_LAYOUT.byteOffset(PathElement.groupElement("unique_id"));
        MATCH_LUCENE_STATE_OFFSET_NUM_RECORDS = MATCH_LUCENE_STATE_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));

        MATCH_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("pmatch_tree"),
                ValueLayout.JAVA_LONG.withName("pwe_params"),
                ValueLayout.JAVA_LONG.withName("match_bm_address"),
                ValueLayout.JAVA_LONG.withName("lucene_bm_address"),
                ValueLayout.JAVA_LONG.withName("pmatch_collect_infos"),
                RowGroupData.FILE_COOKIE_LAYOUT.withName("file_cookie"),
                MATCH_LUCENE_STATE_LAYOUT.withName("lucene_state"),
                ValueLayout.JAVA_INT.withName("nrecs"),
                ValueLayout.JAVA_INT.withName("min_offset"),
                ValueLayout.JAVA_INT.withName("match_collect_buff_ix"),
                ValueLayout.JAVA_INT.withName("tx_id"),
                ValueLayout.JAVA_BYTE.withName("nwes"),
                ValueLayout.JAVA_BYTE.withName("nmatch_collect"),
                ValueLayout.JAVA_BYTE.withName("nmultiple_match"),
                ValueLayout.JAVA_BYTE.withName("max_height")).withName("match_state_t");
        MATCH_STATE_OFFSET_MATCH_TREE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pmatch_tree"));
        MATCH_STATE_OFFSET_WARMUP_ELEMENT_PARAMS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pwe_params"));
        MATCH_STATE_OFFSET_MATCH_BM = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("match_bm_address"));
        MATCH_STATE_OFFSET_LUCENE_BM = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("lucene_bm_address"));
        MATCH_STATE_OFFSET_MATCH_COLLECT_MD = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pmatch_collect_infos"));
        MATCH_STATE_OFFSET_FILE_COOKIE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("file_cookie"));
        MATCH_STATE_OFFSET_LUCENE_STATE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("lucene_state"));
        MATCH_STATE_OFFSET_NUM_RECORDS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));
        MATCH_STATE_OFFSET_MIN_FILE_OFFSET = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("min_offset"));
        MATCH_STATE_OFFSET_MATCH_COLLECT_ID = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("match_collect_buff_ix"));
        MATCH_STATE_OFFSET_TX_ID = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("tx_id"));
        MATCH_STATE_OFFSET_NUM_WARM_UP_ELEMENTS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nwes"));
        MATCH_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nmatch_collect"));
        MATCH_STATE_OFFSET_NUM_CHUNKS_IN_RANGE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nmultiple_match"));
        MATCH_STATE_OFFSET_MAX_TREE_HEIGHT = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("max_height"));
    }

    public MatchState(MemorySegment matchStateMem,
            QueryArgs queryArgs,
            AggregatorPageArgs aggregatorPageArgs,
            Optional<MemorySegment> luceneBitmaps,
            int payloadSize) // payload is taken at the begining of the memory layout
    {
        QueryParams queryParams = queryArgs.queryParams();

        this.matchStateWithPayload = matchStateMem;
        this.matchState = matchStateWithPayload.asSlice(payloadSize, MATCH_STATE_LAYOUT);

        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_MATCH_TREE, queryParams.getMatchNodeAtts().address());
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_WARMUP_ELEMENT_PARAMS, queryParams.getWarmUpElementMatchParams().get().address());
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_MATCH_BM, aggregatorPageArgs.matchBitmaps().map(m -> m.address()).orElse(0L));
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_LUCENE_BM, luceneBitmaps.map(m -> m.address()).orElse(0L));
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_MATCH_COLLECT_MD, queryArgs.matchCollectMetadata().map(m -> m.address()).orElse(0L));
        RowGroupData.setFileCookie(matchState.asSlice(MATCH_STATE_OFFSET_FILE_COOKIE, RowGroupData.FILE_COOKIE_LAYOUT),
                (int) queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FD.ordinal()],
                queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()],
                queryArgs.txArgs().fileCookie()[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()]);
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_NUM_RECORDS, queryParams.getTotalNumRecords());
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_MIN_FILE_OFFSET, queryParams.getMinMatchOffset());
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_MATCH_COLLECT_ID, queryParams.getMatchCollectId());
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_TX_ID, aggregatorPageArgs.queryMemoryId());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_NUM_WARM_UP_ELEMENTS, (byte) queryParams.getNumMatchElements());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS, (byte) queryParams.getNumMatchCollect());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_NUM_CHUNKS_IN_RANGE, (byte) queryArgs.numChunksInRange());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_MAX_TREE_HEIGHT, (byte) queryParams.getMatchTreeHeight());
    }

    // returns the main memory with the payload
    public MemorySegment getMemory()
    {
        return matchStateWithPayload;
    }

    public MemorySegment getMatchLuceneState()
    {
        return matchState.asSlice(MATCH_STATE_OFFSET_LUCENE_STATE, MATCH_LUCENE_STATE_LAYOUT);
    }

    public int getLuceneUniqueId(MemorySegment luceneState)
    {
        return luceneState.get(ValueLayout.JAVA_INT, MATCH_LUCENE_STATE_OFFSET_UNIQUE_ID);
    }

    public int getLuceneNumRecords(MemorySegment luceneState)
    {
        return luceneState.get(ValueLayout.JAVA_INT, MATCH_LUCENE_STATE_OFFSET_NUM_RECORDS);
    }
}
