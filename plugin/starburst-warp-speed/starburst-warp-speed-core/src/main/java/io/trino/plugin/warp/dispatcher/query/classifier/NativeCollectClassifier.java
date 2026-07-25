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
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

        final int remainingTxMemoryFromMatch =
                (maxMatchColumns - Math.min(queryContext.getMatchLeavesDFS().size(), maxMatchColumns)) * matchTxSize;
        final int collectTxMaxMemory = this.collectTxMaxMemoryConfig + remainingTxMemoryFromMatch;
        final int matchCollectBufferSize = queryContext.getEnableMatchCollect() ? this.matchCollectBufferSize : 0;
        NativeCollectState state = new NativeCollectState(collectTxMaxMemory, matchCollectBufferSize);

        Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
        List<NativeQueryCollectData> matchCollectDataList = new ArrayList<>();

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

            collectOptional.ifPresent(matchCollectDataList::add);
        }

        return queryContext.asBuilder()
                .nativeQueryCollectDataList(createMemoryLimitedNativeQueryCollectDataList(
                        queryContext,
                        matchCollectDataList,
                        remainingCollectColumnByBlockIndex,
                        collectTxMaxMemory))
                .remainingCollectColumnByBlockIndex(remainingCollectColumnByBlockIndex)
                .build();
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
        private final int collectTxMaxMemory;
        private final int matchCollectBufferSize;
        private RegularColumn currentColumn;
        private Type currentColumnType;
        private int currentBlockIndex;
        private int collectRecordBufferMemory;
        private int collectTxMemory;
        private int matchCollectMemory;

        NativeCollectState(int collectTxMaxMemory, int matchCollectBufferSize)
        {
            this.collectTxMaxMemory = collectTxMaxMemory;
            this.matchCollectBufferSize = matchCollectBufferSize;
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
