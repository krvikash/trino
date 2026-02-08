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
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;

public class MatchState
{
    private static final long PAGE_BM_ALIGN = 32; // this is the alignment required for intel optimized bitmap operations

    // these definitions can be used by ChunksQueue as well
    static final StructLayout MATCH_BITMAP_DESC_LAYOUT;
    static final long MATCH_BITMAP_DESC_OFFSET_BM_ADDRESS;
    static final long MATCH_BITMAP_DESC_OFFSET_RESET_POINT;
    //private static final long MATCH_BITMAP_DESC_OFFSET_POP_COUNT;

    private static final StructLayout MATCH_LUCENE_STATE_LAYOUT;
    private static final long MATCH_LUCENE_STATE_OFFSET_UNIQUE_ID;
    private static final long MATCH_LUCENE_STATE_OFFSET_NUM_RECORDS;

    static final StructLayout MATCH_STATE_LAYOUT;
    private static final long MATCH_STATE_OFFSET_MATCH_TREE;
    private static final long MATCH_STATE_OFFSET_WARMUP_ELEMENT_PARAMS;
    private static final long MATCH_STATE_OFFSET_ROOT_BITMAPS;
    private static final long MATCH_STATE_OFFSET_NODE_BITMAPS;
    private static final long MATCH_STATE_OFFSET_CURR_BITMAPS;
    private static final long MATCH_STATE_OFFSET_LUCENE_BM;
    private static final long MATCH_STATE_OFFSET_MATCH_COLLECT_MD;
    private static final long MATCH_STATE_OFFSET_FILE_COOKIE;
    private static final long MATCH_STATE_OFFSET_LUCENE_STATE;
    private static final long MATCH_STATE_OFFSET_NUM_RECORDS;
    private static final long MATCH_STATE_OFFSET_MIN_FILE_OFFSET;
    private static final long MATCH_STATE_OFFSET_MATCH_COLLECT_ID;
    private static final long MATCH_STATE_OFFSET_NUM_WARM_UP_ELEMENTS;
    private static final long MATCH_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS;
    private static final long MATCH_STATE_OFFSET_NUM_CHUNKS_IN_RANGE;
    private static final long MATCH_STATE_OFFSET_MAX_TREE_HEIGHT;

    private final int pageSize;
    private final int numBitmaps;
    private final int numLuceneBitmaps;
    private MemorySegment matchState;
    private MemorySegment matchStateWithPayload;
    private Optional<SequenceLayout> flatLevelBitmapsLayout;
    private Optional<MemorySegment> matchBitmaps;
    private Optional<MemorySegment> matchBitmapsDescriptors;
    private Optional<MemorySegment> luceneBitmaps;

    static {
        MATCH_BITMAP_DESC_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("bm"),
                ValueLayout.JAVA_INT.withName("reset_point"),
                ValueLayout.JAVA_INT.withName("pop_count")).withName("page_bm_t");
        MATCH_BITMAP_DESC_OFFSET_BM_ADDRESS = MATCH_BITMAP_DESC_LAYOUT.byteOffset(PathElement.groupElement("bm"));
        MATCH_BITMAP_DESC_OFFSET_RESET_POINT = MATCH_BITMAP_DESC_LAYOUT.byteOffset(PathElement.groupElement("reset_point"));
        //MATCH_BITMAP_DESC_OFFSET_POP_COUNT = MATCH_BITMAP_DESC_LAYOUT.byteOffset(PathElement.groupElement("pop_count"));

        MATCH_LUCENE_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("unique_id"),
                ValueLayout.JAVA_INT.withName("nrecs")).withName("match_lucene_state_t");
        MATCH_LUCENE_STATE_OFFSET_UNIQUE_ID = MATCH_LUCENE_STATE_LAYOUT.byteOffset(PathElement.groupElement("unique_id"));
        MATCH_LUCENE_STATE_OFFSET_NUM_RECORDS = MATCH_LUCENE_STATE_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));

        MATCH_STATE_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("pmatch_tree"),
                ValueLayout.JAVA_LONG.withName("pwe_params"),
                ValueLayout.JAVA_LONG.withName("proot_bms"),
                ValueLayout.JAVA_LONG.withName("pnode_bms"),
                ValueLayout.JAVA_LONG.withName("pcurr_bms"),
                ValueLayout.JAVA_LONG.withName("lucene_bm_address"),
                ValueLayout.JAVA_LONG.withName("pmatch_collect_infos"),
                RowGroupData.FILE_COOKIE_LAYOUT.withName("file_cookie"),
                MATCH_LUCENE_STATE_LAYOUT.withName("lucene_state"),
                ValueLayout.JAVA_INT.withName("nrecs"),
                ValueLayout.JAVA_INT.withName("min_offset"),
                ValueLayout.JAVA_INT.withName("match_collect_buff_ix"),
                ValueLayout.JAVA_BYTE.withName("nwes"),
                ValueLayout.JAVA_BYTE.withName("nmatch_collect"),
                ValueLayout.JAVA_BYTE.withName("nmultiple_match"),
                ValueLayout.JAVA_BYTE.withName("max_height")).withName("match_state_t");
        MATCH_STATE_OFFSET_MATCH_TREE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pmatch_tree"));
        MATCH_STATE_OFFSET_WARMUP_ELEMENT_PARAMS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pwe_params"));
        MATCH_STATE_OFFSET_ROOT_BITMAPS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("proot_bms"));
        MATCH_STATE_OFFSET_NODE_BITMAPS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pnode_bms"));
        MATCH_STATE_OFFSET_CURR_BITMAPS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pcurr_bms"));
        MATCH_STATE_OFFSET_LUCENE_BM = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("lucene_bm_address"));
        MATCH_STATE_OFFSET_MATCH_COLLECT_MD = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("pmatch_collect_infos"));
        MATCH_STATE_OFFSET_FILE_COOKIE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("file_cookie"));
        MATCH_STATE_OFFSET_LUCENE_STATE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("lucene_state"));
        MATCH_STATE_OFFSET_NUM_RECORDS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));
        MATCH_STATE_OFFSET_MIN_FILE_OFFSET = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("min_offset"));
        MATCH_STATE_OFFSET_MATCH_COLLECT_ID = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("match_collect_buff_ix"));
        MATCH_STATE_OFFSET_NUM_WARM_UP_ELEMENTS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nwes"));
        MATCH_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nmatch_collect"));
        MATCH_STATE_OFFSET_NUM_CHUNKS_IN_RANGE = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("nmultiple_match"));
        MATCH_STATE_OFFSET_MAX_TREE_HEIGHT = MATCH_STATE_LAYOUT.byteOffset(PathElement.groupElement("max_height"));
    }

    public MatchState(QueryArgs queryArgs,
            ThreadArena arena,
            Optional<MemorySegment> matchCollectMetadata,
            int payloadSize, // payload is taken at the begining of the memory layout
            int storageBufferMetadaSize, // this memory is allocated as a buffer following the match state struct
            int pageSize)
    {
        QueryParams queryParams = queryArgs.queryParams();
        // +1 below is for the current bitmaps set that is used as an intermediate bitmap by native layer
        // +1 more below is for safety we allocate more memory than needed and allow native to support a one level higher tree than java limit
        final int matchTreeHeight = queryParams.getRootMatchNode().map(r -> r.getHeight() + 2).orElse(0);
        this.pageSize = pageSize;
        this.numBitmaps = queryArgs.numChunksInRange() * matchTreeHeight;
        this.numLuceneBitmaps = queryArgs.numChunksInRange() * queryParams.getNumLucene();
        this.matchBitmaps = Optional.empty();
        this.matchBitmapsDescriptors = Optional.empty();
        this.luceneBitmaps = Optional.empty();
        setMemory(arena, payloadSize, queryArgs.numChunksInRange(), storageBufferMetadaSize);
        setState(queryArgs, matchCollectMetadata);
    }

    private void setMemory(ThreadArena arena, int payloadSize, int numChunksInRange, int storageBufferMetadaSize)
    {
        if (numBitmaps > 0) {
            matchBitmaps = Optional.of(arena.allocate((long) numBitmaps * (long) pageSize, PAGE_BM_ALIGN));
            matchBitmapsDescriptors = Optional.of(arena.allocate(MemoryLayout.sequenceLayout(numBitmaps, MATCH_BITMAP_DESC_LAYOUT).byteSize(), ValueLayout.JAVA_INT.byteSize()));
            flatLevelBitmapsLayout = Optional.of(MemoryLayout.sequenceLayout(numChunksInRange, MATCH_BITMAP_DESC_LAYOUT));
        }
        if (numLuceneBitmaps > 0) {
            luceneBitmaps = Optional.of(arena.allocate((long) numLuceneBitmaps * (long) pageSize, PAGE_BM_ALIGN));
        }
        this.matchStateWithPayload = arena.allocate(payloadSize + MATCH_STATE_LAYOUT.byteSize() + storageBufferMetadaSize, ValueLayout.JAVA_LONG.byteSize());
        this.matchState = matchStateWithPayload.asSlice(payloadSize, MATCH_STATE_LAYOUT);
    }

    private void setState(QueryArgs queryArgs, Optional<MemorySegment> matchCollectMetadata)
    {
        QueryParams queryParams = queryArgs.queryParams();
        long[] fileCookie = queryArgs.fileCookie();
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_MATCH_TREE, queryParams.getMatchNodeAtts().address());
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_WARMUP_ELEMENT_PARAMS, queryParams.getWarmUpElementMatchParams().get().address());

        if (matchBitmapsDescriptors.isPresent()) {
            MemorySegment allBitmaps = matchBitmapsDescriptors.get();
            SequenceLayout flatLevelBitmaps = flatLevelBitmapsLayout.get();
            final long flatLevelBitmapsSize = flatLevelBitmaps.byteSize();
            List<MemorySegment> allBitmapsDescriptors = new ArrayList<>();

            // we first cut the fixed size arrays which are the root and current of size flatLevelBitmapsLayout
            // then we cut the node bitnmaps as all the rest.
            MemorySegment rootBitmaps = allBitmaps.asSlice(0, flatLevelBitmaps);
            rootBitmaps.elements(MATCH_BITMAP_DESC_LAYOUT).forEach(bm -> allBitmapsDescriptors.add(bm));
            matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_ROOT_BITMAPS, rootBitmaps.address());

            MemorySegment currBitmaps = allBitmaps.asSlice(flatLevelBitmapsSize, flatLevelBitmaps);
            currBitmaps.elements(MATCH_BITMAP_DESC_LAYOUT).forEach(bm -> allBitmapsDescriptors.add(bm));
            matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_CURR_BITMAPS, currBitmaps.address());

            MemorySegment nodeBitmaps = allBitmaps.asSlice(flatLevelBitmapsSize * 2);
            nodeBitmaps.elements(MATCH_BITMAP_DESC_LAYOUT).forEach(bm -> allBitmapsDescriptors.add(bm));
            matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_NODE_BITMAPS, nodeBitmaps.address());

            long bitmapAddress = matchBitmaps.get().address();
            for (MemorySegment bitmapDescriptor : allBitmapsDescriptors) {
                bitmapDescriptor.set(ValueLayout.JAVA_LONG, MATCH_BITMAP_DESC_OFFSET_BM_ADDRESS, bitmapAddress);
                bitmapAddress += pageSize;
            }
        }
        else {
            matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_ROOT_BITMAPS, 0L);
            matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_CURR_BITMAPS, 0L);
            matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_NODE_BITMAPS, 0L);
        }

        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_LUCENE_BM, luceneBitmaps.map(m -> m.address()).orElse(0L));
        matchState.set(ValueLayout.JAVA_LONG, MATCH_STATE_OFFSET_MATCH_COLLECT_MD, matchCollectMetadata.map(m -> m.address()).orElse(0L));
        RowGroupData.setFileCookie(matchState.asSlice(MATCH_STATE_OFFSET_FILE_COOKIE, RowGroupData.FILE_COOKIE_LAYOUT),
                (int) fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()],
                fileCookie[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()],
                fileCookie[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()]);
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_NUM_RECORDS, queryParams.getTotalNumRecords());
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_MIN_FILE_OFFSET, queryParams.getMinMatchOffset());
        matchState.set(ValueLayout.JAVA_INT, MATCH_STATE_OFFSET_MATCH_COLLECT_ID, queryParams.getMatchCollectId());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_NUM_WARM_UP_ELEMENTS, (byte) queryParams.getNumMatchElements());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_NUM_MATCH_COLLECT_ELEMENTS, (byte) queryParams.getNumMatchCollect());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_NUM_CHUNKS_IN_RANGE, (byte) queryArgs.numChunksInRange());
        matchState.set(ValueLayout.JAVA_BYTE, MATCH_STATE_OFFSET_MAX_TREE_HEIGHT, (byte) queryParams.getMatchTreeHeight());
    }

    // returns the main memory with the payload
    public MemorySegment getStateMemory()
    {
        return matchStateWithPayload;
    }

    public Optional<MemorySegment> getMatchBitmaps()
    {
        return matchBitmaps;
    }

    public Optional<MemorySegment> getLuceneBitmaps()
    {
        return luceneBitmaps;
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

    public List<MemorySegment> getRootBitmapsDescriptors()
    {
        return matchBitmapsDescriptors.map(bm -> bm.asSlice(0, flatLevelBitmapsLayout.get()).elements(MATCH_BITMAP_DESC_LAYOUT).toList()).orElse(Collections.emptyList());
    }
}
