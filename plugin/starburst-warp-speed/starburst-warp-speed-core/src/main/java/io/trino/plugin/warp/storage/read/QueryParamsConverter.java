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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.MatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.MatchNodeType;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.storage.memory.GcArena;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.spi.block.Block;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.ValueLayout;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.dispatcher.query.MatchCollectUtils.findMatchForMatchCollect;
import static io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier.INVALID_TOTAL_RECORDS;
import static io.trino.plugin.warp.storage.read.WarpPageSource.INVALID_COL_IX;

public class QueryParamsConverter
{
    private static final Logger logger = Logger.get(QueryParamsConverter.class);
    private static final int MATCH = 0;
    private static final int COLLECT = 1;

    private QueryParamsConverter() {}

    public static QueryParams createQueryParams(
            WorkerMemoryManager workerMemoryManager,
            QueryContext queryContext,
            String filePath,
            long fileModTime,
            boolean rangesRequired,
            String queryId)
    {
        checkArgument(queryContext.getTotalRecords() != INVALID_TOTAL_RECORDS, "Invalid total records");

        Optional<MatchData> matchData = queryContext.getMatchData();
        ImmutableList.Builder<PredicateCacheData> predicateCacheDataBuilder = ImmutableList.builder();

        GcArena arena = workerMemoryManager.getGcArena();
        long lastUsedTimestamp = Instant.now().toEpochMilli();
        int[] minOffsets = new int[2];
        minOffsets[COLLECT] = Integer.MAX_VALUE;
        minOffsets[MATCH] = Integer.MAX_VALUE;

        // prepare match elements list
        List<QueryMatchData> queryMatchDataLeaves = matchData.stream()
                .flatMap(queryMatchData -> queryMatchData.getLeavesDFS().stream())
                .toList();
        int numLeaves = queryMatchDataLeaves.size();
        int subtreeSize = matchData.map(data -> data.getSubtreeSize()).orElse(0);
        boolean onlyLeaves = (numLeaves > 0) && (numLeaves == subtreeSize);
        if (onlyLeaves) {
            subtreeSize++; // one more for an AND root we will add above
        }

        // collect allocate parameters
        int numCollectElements = queryContext.getNativeQueryCollectDataList().size();
        Optional<MemorySegment> warmUpElementCollectParams = (numCollectElements > 0) ? Optional.of(allocateWarmUpElementCollectParamsMemory(arena, numCollectElements)) : Optional.empty();
        ArrayDeque<MemorySegment> warmUpElementCollectParamsQueue = sliceWarmUpElementCollectParamsMemory(warmUpElementCollectParams, numCollectElements);
        // collect create paramters
        CollectAndMatchCollectParams collectAndMatchCollectParams = getCollectAndMatchCollectParams(
                queryMatchDataLeaves,
                queryContext.getNativeQueryCollectDataList(),
                warmUpElementCollectParamsQueue,
                lastUsedTimestamp,
                minOffsets);

        // match allocate parameters
        Optional<MemorySegment> warmUpElementMatchParams = (numLeaves > 0) ? Optional.of(allocateWarmUpElementMatchParamsMemory(arena, numLeaves)) : Optional.empty();
        ArrayDeque<MemorySegment> warmUpElementMatchParamsQueue = sliceWarmUpElementMatchParamsMemory(warmUpElementMatchParams, numLeaves);
        // match allocate node attributes
        MemorySegment matchNodeAtts = allocateMatchNodeAttsMemory(arena, subtreeSize);
        ArrayDeque<MemorySegment> matchNodeAttsQueue = sliceMatchNodeAttsMemory(matchNodeAtts, subtreeSize);
        Optional<MemorySegment> rootMatchNodeAtt = onlyLeaves ? Optional.of(matchNodeAttsQueue.remove()) : Optional.empty();

        // match recursively create tree nodes
        MatchNodes matchNodes = matchData.map(data -> convertToMatchNodes(
                List.of(data),
                collectAndMatchCollectParams.matchCollectElements(),
                lastUsedTimestamp,
                0,
                0,
                predicateCacheDataBuilder,
                minOffsets,
                warmUpElementMatchParamsQueue,
                matchNodeAttsQueue)).orElseGet(() -> new MatchNodes(Collections.emptyList(), 0, 0));
        // calculate the root
        Optional<MatchNode> rootMatchNode = createRootMatchNode(matchNodes.terms(), rootMatchNodeAtt);

        logger.debug(
                "numLeaves %d subtreeSize %d height %d onlyLeaves %b",
                numLeaves,
                subtreeSize,
                rootMatchNode.map(node -> node.getHeight()).orElse(0),
                onlyLeaves);
        return new QueryParams(
                rootMatchNode,
                warmUpElementMatchParams,
                matchNodeAtts,
                warmUpElementCollectParams,
                matchNodes.numLucene(),
                collectAndMatchCollectParams.matchCollectId(),
                collectAndMatchCollectParams.collectParamsList(),
                queryContext.getTotalRecords(),
                minOffsets[MATCH],
                minOffsets[COLLECT],
                filePath,
                fileModTime,
                predicateCacheDataBuilder.build(),
                rangesRequired,
                arena,
                queryId);
    }

    private static MemorySegment allocateWarmUpElementCollectParamsMemory(GcArena arena, int numElements)
    {
        SequenceLayout warmUpElementCollectParamsLayout = MemoryLayout.sequenceLayout(numElements, WarmupElementCollectParams.WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT);
        return arena.allocate(warmUpElementCollectParamsLayout.byteSize(), ValueLayout.JAVA_SHORT.byteSize());
    }

    private static ArrayDeque<MemorySegment> sliceWarmUpElementCollectParamsMemory(Optional<MemorySegment> warmUpElementCollectParams, int numElements)
    {
        ArrayDeque<MemorySegment> warmUpElementCollectParamsQueue = new ArrayDeque<>(numElements);
        if (warmUpElementCollectParams.isPresent()) {
            warmUpElementCollectParams.get().elements(WarmupElementCollectParams.WARMUP_ELEMENT_COLLECT_PARAMS_LAYOUT).forEach(m -> warmUpElementCollectParamsQueue.add(m));
        }
        return warmUpElementCollectParamsQueue;
    }

    private static MemorySegment allocateWarmUpElementMatchParamsMemory(GcArena arena, int numLeaves)
    {
        SequenceLayout warmUpElementMatchParamsLayout = MemoryLayout.sequenceLayout(numLeaves, WarmupElementMatchParams.WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT);
        return arena.allocate(warmUpElementMatchParamsLayout.byteSize(), ValueLayout.JAVA_INT.byteSize());
    }

    private static ArrayDeque<MemorySegment> sliceWarmUpElementMatchParamsMemory(Optional<MemorySegment> warmUpElementMatchParams, int numLeaves)
    {
        ArrayDeque<MemorySegment> warmUpElementMatchParamsQueue = new ArrayDeque<>(numLeaves);
        if (warmUpElementMatchParams.isPresent()) {
            warmUpElementMatchParams.get().elements(WarmupElementMatchParams.WARMUP_ELEMENT_MATCH_PARAMS_LAYOUT).forEach(m -> warmUpElementMatchParamsQueue.add(m));
        }
        return warmUpElementMatchParamsQueue;
    }

    private static MemorySegment allocateMatchNodeAttsMemory(GcArena arena, int subtreeSize)
    {
        SequenceLayout matchNodeAttsLayout = MemoryLayout.sequenceLayout(subtreeSize, MatchNodeAtt.MATCH_NODE_ATT_LAYOUT);
        return arena.allocate(matchNodeAttsLayout.byteSize(), ValueLayout.JAVA_BYTE.byteSize());
    }

    private static ArrayDeque<MemorySegment> sliceMatchNodeAttsMemory(MemorySegment matchNodeAtts, int subtreeSize)
    {
        ArrayDeque<MemorySegment> matchNodeAttsQueue = new ArrayDeque<>(subtreeSize);
        matchNodeAtts.elements(MatchNodeAtt.MATCH_NODE_ATT_LAYOUT).forEach(m -> matchNodeAttsQueue.add(m));
        return matchNodeAttsQueue;
    }

    private static Optional<MatchNode> createRootMatchNode(List<MatchNode> matchNodes, Optional<MemorySegment> rootMatchNodeAtt)
    {
        if (matchNodes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rootMatchNodeAtt.map(m -> (MatchNode) new LogicalMatchNode(MatchNodeType.MATCH_NODE_TYPE_AND, matchNodes, m)).orElse(matchNodes.getFirst()));
    }

    private static MatchNodes convertToMatchNodes(
            List<MatchData> matchDataList,
            List<MatchCollectElement> matchCollectElements,
            long lastUsedTimestamp,
            int currentNumLeaves,
            int currentNumLucene,
            ImmutableList.Builder<PredicateCacheData> predicateCacheDataBuilder,
            int[] minOffsets,
            ArrayDeque<MemorySegment> warmUpElementMatchParamsQueue,
            ArrayDeque<MemorySegment> matchNodeAttsQueue)
    {
        List<MatchNode> convertedList = new ArrayList<>(matchDataList.size());

        for (MatchData matchData : matchDataList) {
            if (matchData instanceof QueryMatchData queryMatchData) {
                WarmUpElement matchDataWarmUpElement = queryMatchData.getWarmUpElement();
                matchDataWarmUpElement.setUsedTimestamp(lastUsedTimestamp);
                if (minOffsets[MATCH] > matchDataWarmUpElement.getStartOffset()) {
                    minOffsets[MATCH] = matchDataWarmUpElement.getStartOffset();
                }

                Optional<WarmupElementLuceneParams> luceneParams;
                if (queryMatchData instanceof LuceneQueryMatchData luceneQueryMatchData) {
                    luceneParams = Optional.of(new WarmupElementLuceneParams(luceneQueryMatchData, currentNumLucene));
                    currentNumLucene++;
                }
                else {
                    luceneParams = Optional.empty();
                }

                Optional<MatchCollectElement> matchCollectElement = matchCollectElements.stream()
                        .filter(match -> match.getQueryMatchData().equals(queryMatchData))
                        .findFirst();
                int matchCollectIndex = matchCollectElement.map(MatchCollectElement::getMatchCollectIndex).orElse(INVALID_COL_IX);
                MatchCollectOp matchCollectOp = matchCollectElement.map(MatchCollectElement::getMatchCollectOp).orElse(MatchCollectOp.MATCH_COLLECT_OP_INVALID);

                convertedList.add(
                        new WarmupElementMatchParams(
                                warmUpElementMatchParamsQueue.remove(),
                                queryMatchData.getPredicateCacheData().getPredicateBufferInfo().buff(),
                                matchDataWarmUpElement.getQueryOffset(),
                                matchDataWarmUpElement.getRecTypeCode(),
                                matchDataWarmUpElement.getRecTypeLength(),
                                matchDataWarmUpElement.getWarmUpType(),
                                matchDataWarmUpElement.getQueryReadSize(),
                                matchCollectOp,
                                matchCollectIndex,
                                matchDataWarmUpElement.getWarmupElementStats().getNullsCount() > 0 && queryMatchData.isCollectNulls(),
                                queryMatchData.isTightnessRequired(),
                                matchDataWarmUpElement.getWarmEvents(),
                                matchDataWarmUpElement.isImported(),
                                luceneParams,
                                matchNodeAttsQueue.remove(),
                                currentNumLeaves));
                currentNumLeaves++;
                predicateCacheDataBuilder.add(queryMatchData.getPredicateCacheData());
            }
            else if (matchData instanceof LogicalMatchData logicalMatchData) {
                MemorySegment matchNodeAtt = matchNodeAttsQueue.remove();
                MatchNodes matchNodes = convertToMatchNodes(
                        logicalMatchData.getTerms(),
                        matchCollectElements,
                        lastUsedTimestamp,
                        currentNumLeaves,
                        currentNumLucene,
                        predicateCacheDataBuilder,
                        minOffsets,
                        warmUpElementMatchParamsQueue,
                        matchNodeAttsQueue);
                MatchNodeType nodeType = logicalMatchData.getOperator() == LogicalMatchData.Operator.AND ? MatchNodeType.MATCH_NODE_TYPE_AND : MatchNodeType.MATCH_NODE_TYPE_OR;
                convertedList.add(new LogicalMatchNode(nodeType, matchNodes.terms(), matchNodeAtt));
                currentNumLeaves = matchNodes.numLeaves();
                currentNumLucene = matchNodes.numLucene();
            }
            else {
                throw new RuntimeException("Unknown MatchData type: " + matchData);
            }
        }
        return new MatchNodes(convertedList, currentNumLeaves, currentNumLucene);
    }

    private static CollectAndMatchCollectParams getCollectAndMatchCollectParams(
            List<QueryMatchData> queryMatchDataLeaves,
            ImmutableList<NativeQueryCollectData> nativeQueryCollectDataList,
            ArrayDeque<MemorySegment> warmUpElementCollectParamsQueue,
            long lastUsedTimestamp,
            int[] minOffsets)
    {
        List<WarmupElementCollectParams> collectParamsList = new ArrayList<>();
        List<MatchCollectElement> matchCollectElements = new ArrayList<>();
        int matchCollectId = MatchCollectIdService.INVALID_ID;

        for (NativeQueryCollectData nativeQueryCollectData : nativeQueryCollectDataList) {
            WarmUpElement collectDataWarmUpElement = nativeQueryCollectData.getWarmUpElement();
            collectDataWarmUpElement.setUsedTimestamp(lastUsedTimestamp);
            if (minOffsets[COLLECT] > collectDataWarmUpElement.getStartOffset()) {
                minOffsets[COLLECT] = collectDataWarmUpElement.getStartOffset();
            }

            boolean isCollectNulls = collectDataWarmUpElement.getWarmupElementStats().getNullsCount() > 0;
            int blockIndex = nativeQueryCollectDataList.indexOf(nativeQueryCollectData);
            int matchCollectIndex = INVALID_COL_IX;
            int currMatchCollectId = nativeQueryCollectData.getMatchCollectId();
            if (currMatchCollectId != matchCollectId) {
                if (matchCollectId != MatchCollectIdService.INVALID_ID) {
                    throw new RuntimeException("match collect id is not the same for all elements " + matchCollectId + " and " + currMatchCollectId);
                }
                matchCollectId = currMatchCollectId;
            }
            Optional<Block> valuesDictBlock = Optional.empty();
            if (nativeQueryCollectData.getMatchCollectType() != MatchCollectUtils.MatchCollectType.DISABLED) {
                QueryMatchData queryMatchData = findMatchForMatchCollect(nativeQueryCollectData, queryMatchDataLeaves)
                        .orElseThrow(() -> new RuntimeException("Expected match-collect was not found"));
                isCollectNulls &= queryMatchData.getWarmUpElement().getWarmupElementStats().getNullsCount() > 0 && queryMatchData.isCollectNulls();
                matchCollectIndex = matchCollectElements.size();
                boolean mappedMatchCollect = nativeQueryCollectData.getMatchCollectType() == MatchCollectUtils.MatchCollectType.MAPPED;
                if (mappedMatchCollect) {
                    valuesDictBlock = queryMatchData.getPredicateCacheData().getValuesDict();
                }
                matchCollectElements.add(new MatchCollectElement(queryMatchData, matchCollectIndex, mappedMatchCollect));
            }

            collectParamsList.add(
                    new WarmupElementCollectParams(
                            warmUpElementCollectParamsQueue.remove(),
                            collectDataWarmUpElement.getQueryOffset(),
                            collectDataWarmUpElement.getRecTypeCode(),   // native will use the original code
                            collectDataWarmUpElement.getRecTypeLength(), // native will use the original length
                            collectDataWarmUpElement.getWarmUpType(),
                            collectDataWarmUpElement.getQueryReadSize(),
                            matchCollectIndex,
                            isCollectNulls,
                            collectDataWarmUpElement.getWarmId(),
                            // page block should hold the original code and length
                            collectDataWarmUpElement.getRecTypeCode(),
                            collectDataWarmUpElement.getRecTypeLength(),
                            collectDataWarmUpElement.getWarmEvents(),
                            collectDataWarmUpElement.isImported(),
                            blockIndex,
                            valuesDictBlock));
        }
        return new CollectAndMatchCollectParams(collectParamsList, matchCollectElements, matchCollectId);
    }

    private record CollectAndMatchCollectParams(
            List<WarmupElementCollectParams> collectParamsList,
            List<MatchCollectElement> matchCollectElements,
            int matchCollectId) {}

    private record MatchNodes(
            List<MatchNode> terms,
            int numLeaves,
            int numLucene) {}
}
