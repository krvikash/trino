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
package io.trino.plugin.warp.dispatcher.query.classifier;

import com.google.common.collect.Iterables;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.MatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil.canApplyPredicate;

public class NativeCollectClassifier
        implements Classifier
{
    public static final int COLLECT_BUFFER_MAX_MEMORY = 13000 * 1024;

    private final int matchCollectBufferSize;
    private final int matchCollectRecSizeFactor;
    private final int collectTxMaxMemoryConfig;
    private final int matchTxSize;
    private final int maxMatchColumns;
    private final BufferAllocator bufferAllocator;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    // collectRecordBufferMaxMemory is the maximal memory we can use from the bundle memory for all collect columns record buffers
    // collectTxMaxMemory is the maximal memory we can use for native tx for all collect colulmns
    NativeCollectClassifier(
            int matchCollectBufferSize,
            int matchCollectRecSizeFactor,
            int collectTxMaxMemory,
            int matchTxSize,
            int maxMatchColumns,
            BufferAllocator bufferAllocator,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.matchCollectBufferSize = matchCollectBufferSize;
        this.matchCollectRecSizeFactor = matchCollectRecSizeFactor;
        this.collectTxMaxMemoryConfig = collectTxMaxMemory;
        this.matchTxSize = matchTxSize;
        this.maxMatchColumns = maxMatchColumns;
        this.bufferAllocator = bufferAllocator;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        if (queryContext.getRemainingCollectColumnByBlockIndex().isEmpty()) {
            return queryContext;
        }

        final int freeMatchColumns = Math.max(maxMatchColumns - queryContext.getMatchLeavesDFS().size(), 0);
        final int collectTxMaxMemory = this.collectTxMaxMemoryConfig + (freeMatchColumns * matchTxSize);
        final int matchCollectBufferSize = queryContext.getEnableMatchCollect() ? this.matchCollectBufferSize : 0;
        NativeCollectState state = new NativeCollectState(collectTxMaxMemory, matchCollectBufferSize, freeMatchColumns);

        // an index walk pays for itself only on a scan that already matches on another column
        boolean collectFromIndex = classifyArgs.isCollectFromIndex() && queryContext.getMatchData().isPresent();

        Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
        List<NativeQueryCollectData> matchCollectDataList = new ArrayList<>();
        Map<Integer, QueryMatchData> indexMatchByBlockIndex = new HashMap<>();

        for (Map.Entry<Integer, ColumnHandle> entry : queryContext.getRemainingCollectColumnByBlockIndex().entrySet()) {
            if (!state.isCollectMemoryAvailable()) {
                break;
            }

            state.setCurrentColumn(entry.getValue(), entry.getKey());
            Optional<NativeQueryCollectData> collectOptional = Optional.empty();

            // try match collect first
            Optional<QueryMatchData> potentialMatchForMatchCollect = queryContext.getMatchLeavesDFS().stream()
                    .filter(queryMatchData -> !queryMatchData.isPartOfLogicalOr() && queryMatchData.canMatchCollect(state.getCurrentColumn()))
                    .findFirst();
            if (potentialMatchForMatchCollect.isPresent()) {
                collectOptional = createMatchCollect(classifyArgs, state, potentialMatchForMatchCollect.get());
            }
            else if (collectFromIndex) {
                collectOptional = createCollectFromIndex(classifyArgs, state, indexMatchByBlockIndex);
            }

            collectOptional.ifPresent(matchCollectDataList::add);
        }

        List<NativeQueryCollectData> nativeQueryCollectDataList = createMemoryLimitedNativeQueryCollectDataList(
                queryContext,
                matchCollectDataList,
                remainingCollectColumnByBlockIndex,
                state.getCollectTxMaxMemory());

        return queryContext.asBuilder()
                .nativeQueryCollectDataList(nativeQueryCollectDataList)
                .matchData(addIndexMatches(queryContext.getMatchData(), indexMatchByBlockIndex, nativeQueryCollectDataList))
                .remainingCollectColumnByBlockIndex(remainingCollectColumnByBlockIndex)
                .build();
    }

    // Collects the current column from its index and records the match that collect needs, or gives the match tx back.
    private Optional<NativeQueryCollectData> createCollectFromIndex(
            ClassifyArgs classifyArgs,
            NativeCollectState state,
            Map<Integer, QueryMatchData> indexMatchByBlockIndex)
    {
        Optional<QueryMatchData> indexMatch = createIndexMatch(classifyArgs, state);
        if (indexMatch.isEmpty() || !state.reserveMatchTx()) {
            return Optional.empty();
        }
        Optional<NativeQueryCollectData> collectData = createMatchCollect(classifyArgs, state, indexMatch.get());
        if (collectData.isEmpty()) {
            state.releaseMatchTx();
            return Optional.empty();
        }
        indexMatchByBlockIndex.put(state.getCurrentBlockIndex(), indexMatch.get());
        return collectData;
    }

    // A column with no predicate on it is served from its basic index element by matching it against all values.
    // The native engine copies each value into the match collect buffer while it walks the index.
    private Optional<QueryMatchData> createIndexMatch(ClassifyArgs classifyArgs, NativeCollectState state)
    {
        Type type = state.getCurrentColumnType();
        if (TypeUtils.isStrType(type)) {
            // variable length values are indexed by their crc, so the index holds nothing to collect
            return Optional.empty();
        }
        Optional<WarmUpElement> warmUpElement = classifyArgs.getWarmedWarmupTypes()
                .getByTypeAndColumn(WarmUpType.WARM_UP_TYPE_BASIC, state.getCurrentColumn(), TransformFunction.NONE);
        if (!canApplyPredicate(warmUpElement, type)) {
            return Optional.empty();
        }
        Domain domain = Domain.all(type);
        NativeExpression nativeExpression = NativeExpression.builder()
                .predicateType(PredicateType.PREDICATE_TYPE_ALL)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .domain(domain)
                .build();
        return Optional.of(BasicQueryMatchData.builder()
                .warmUpElement(warmUpElement.get())
                .type(type)
                .domain(Optional.of(domain))
                .nativeExpression(nativeExpression)
                .build());
    }

    // keep only the index matches whose collect survived the memory limit, the rest would walk an index for nothing
    private Optional<MatchData> addIndexMatches(
            Optional<MatchData> matchData,
            Map<Integer, QueryMatchData> indexMatchByBlockIndex,
            List<NativeQueryCollectData> nativeQueryCollectDataList)
    {
        List<MatchData> indexMatches = nativeQueryCollectDataList.stream()
                .map(collectData -> indexMatchByBlockIndex.get(collectData.getBlockIndex()))
                .filter(Objects::nonNull)
                .collect(toImmutableList());
        if (indexMatches.isEmpty()) {
            return matchData;
        }
        List<MatchData> allMatches = Stream.concat(Stream.of(matchData.orElseThrow()), indexMatches.stream())
                .collect(toImmutableList());
        return Optional.of(new LogicalMatchData(LogicalMatchData.Operator.AND, allMatches));
    }

    // note that we have two types of memory to check here:
    // 1. collect memory coming from the bundle that is used to pass the collected records and nulls to java from native
    // 2. match collect memory which is a native intermediate buffer used for collecting the data while traversing the index
    private Optional<NativeQueryCollectData> createMatchCollect(
            ClassifyArgs classifyArgs,
            NativeCollectState state,
            QueryMatchData queryMatchData)
    {
        boolean canMapMatchCollect = classifyArgs.isMappedMatchCollect() && queryMatchData.canMapMatchCollect();
        boolean canMapMatchCollectVarchar = canMapMatchCollect && classifyArgs.isVarcharMappedMatchCollectEnabled();
        if (!state.isMatchCollectMemoryAvailable() || (!canMapMatchCollectVarchar && TypeUtils.isStrType(state.getCurrentColumnType()))) {
            return Optional.empty();
        }

        WarmUpElement warmUpElement = queryMatchData.getWarmUpElement();

        // check feasibility in terms of memory
        final int collectBufferSize = getCollectBufferSize(warmUpElement); // collectBufferSize is always positive
        final int collectTxSize = getCollectTxSize(warmUpElement);
        final int matchCollectBufferSize = getMatchCollectBufferSize(warmUpElement, canMapMatchCollect) * matchCollectRecSizeFactor;
        if ((matchCollectBufferSize > 0) && state.updateCollectMemoryIfAvailable(collectBufferSize, collectTxSize, matchCollectBufferSize)) {
            return Optional.of(NativeQueryCollectData.builder()
                    .warmUpElement(warmUpElement)
                    .type(state.getCurrentColumnType())
                    .blockIndex(state.getCurrentBlockIndex())
                    .matchCollectType(canMapMatchCollect ? MatchCollectType.MAPPED : MatchCollectType.ORDINARY)
                    .build());
        }
        return Optional.empty();
    }

    private List<NativeQueryCollectData> createMemoryLimitedNativeQueryCollectDataList(
            QueryContext queryContext,
            List<NativeQueryCollectData> matchCollectDataList,
            Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex,
            int collectTxMaxMemory)
    {
        int collectRecordBufferMemory = 0;
        int collectTxMemory = 0;
        List<NativeQueryCollectData> nativeQueryCollectDataList = new ArrayList<>();

        // start with those who are already in the list of the context, then the newly classified ones. stop when memory is exhausted
        for (NativeQueryCollectData nativeQueryCollectData : Iterables.concat(queryContext.getNativeQueryCollectDataList(), matchCollectDataList)) {
            collectRecordBufferMemory += getCollectBufferSize(nativeQueryCollectData.getWarmUpElement());
            if (collectRecordBufferMemory > COLLECT_BUFFER_MAX_MEMORY) {
                return nativeQueryCollectDataList;
            }
            collectTxMemory += getCollectTxSize(nativeQueryCollectData.getWarmUpElement());
            if (collectTxMemory > collectTxMaxMemory) {
                return nativeQueryCollectDataList;
            }
            nativeQueryCollectDataList.add(nativeQueryCollectData);
            remainingCollectColumnByBlockIndex.remove(nativeQueryCollectData.getBlockIndex());
        }

        return nativeQueryCollectDataList;
    }

    private int getMatchCollectBufferSize(WarmUpElement warmUpElement, boolean mappedMatchCollect)
    {
        return mappedMatchCollect ? bufferAllocator.getMappedMatchCollectBufferSize() : bufferAllocator.getMatchCollectRecordBufferSize(warmUpElement.getRecTypeLength());
    }

    private int getCollectBufferSize(WarmUpElement warmUpElement)
    {
        // Here we calculate the minimal buffer size needed for a we, while in the actual allocation (CollectTxService.getCollectBuffersAllocationParams)
        // we try to get to the maximal buffer in order to maximize number of records in a page
        return bufferAllocator.getCollectRecordBufferSizeMust(warmUpElement.getRecTypeCode(), warmUpElement.getRecTypeLength()) +
                bufferAllocator.getQueryNullBufferSize(warmUpElement.getRecTypeCode());
    }

    private int getCollectTxSize(WarmUpElement warmUpElement)
    {
        return bufferAllocator.getCollectTxSize(warmUpElement.getRecTypeCode(), warmUpElement.getRecTypeLength());
    }

    private class NativeCollectState
    {
        private final int matchCollectBufferSize;
        private int collectTxMaxMemory;
        private int freeMatchColumns;
        private RegularColumn currentColumn;
        private Type currentColumnType;
        private int currentBlockIndex;
        private int collectRecordBufferMemory;
        private int collectTxMemory;
        private int matchCollectMemory;

        NativeCollectState(int collectTxMaxMemory, int matchCollectBufferSize, int freeMatchColumns)
        {
            this.collectTxMaxMemory = collectTxMaxMemory;
            this.matchCollectBufferSize = matchCollectBufferSize;
            this.freeMatchColumns = freeMatchColumns;
        }

        int getCollectTxMaxMemory()
        {
            return collectTxMaxMemory;
        }

        // an index match costs a match tx, whose memory collectTxMaxMemory counts as free until it is reserved
        boolean reserveMatchTx()
        {
            if ((freeMatchColumns == 0) || (collectTxMaxMemory - matchTxSize < collectTxMemory)) {
                return false;
            }
            freeMatchColumns--;
            collectTxMaxMemory -= matchTxSize;
            return true;
        }

        void releaseMatchTx()
        {
            freeMatchColumns++;
            collectTxMaxMemory += matchTxSize;
        }

        void setCurrentColumn(ColumnHandle columnHandle, int blockIndex)
        {
            currentBlockIndex = blockIndex;
            currentColumn = dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandle);
            currentColumnType = dispatcherProxiedConnectorTransformer.getColumnType(columnHandle);
        }

        RegularColumn getCurrentColumn()
        {
            return currentColumn;
        }

        Type getCurrentColumnType()
        {
            return currentColumnType;
        }

        int getCurrentBlockIndex()
        {
            return currentBlockIndex;
        }

        boolean updateCollectMemoryIfAvailable(int collectBufferUpdate, int collectTxUpdate, int matchCollectBufferUpdate)
        {
            // check if the aggregated memory can take the update as well as the match collect memory
            final int updatedCollectRecordBufferMemory = collectRecordBufferMemory + collectBufferUpdate;
            final int updatedCollectTxMemory = collectTxMemory + collectTxUpdate;
            final int updatedMatchCollectMemory = matchCollectMemory + matchCollectBufferUpdate;
            if ((updatedCollectRecordBufferMemory > COLLECT_BUFFER_MAX_MEMORY) ||
                    (updatedMatchCollectMemory > matchCollectBufferSize) ||
                    (updatedCollectTxMemory > collectTxMaxMemory)) {
                return false;
            }

            collectRecordBufferMemory = updatedCollectRecordBufferMemory;
            collectTxMemory = updatedCollectTxMemory;
            matchCollectMemory = updatedMatchCollectMemory;
            return true;
        }

        // as long as there is "room" to take more columns we must continue to look for more collect elements. we use this method to know when to stop the main loop
        boolean isCollectMemoryAvailable()
        {
            return (collectRecordBufferMemory < COLLECT_BUFFER_MAX_MEMORY) && (collectTxMemory < collectTxMaxMemory);
        }

        boolean isMatchCollectMemoryAvailable()
        {
            return (matchCollectMemory < matchCollectBufferSize);
        }
    }
}
