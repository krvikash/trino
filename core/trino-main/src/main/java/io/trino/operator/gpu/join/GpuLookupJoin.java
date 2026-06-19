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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.GatherMap;
import ai.rapids.cudf.NullEquality;
import ai.rapids.cudf.OutOfBoundsPolicy;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.join.GpuJoinBridge.EmptyBuildSide;
import io.trino.operator.gpu.join.GpuJoinBridge.FilteredHashJoinBridge;
import io.trino.operator.gpu.join.GpuJoinBridge.HashJoinBridge;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuLookupJoin
        implements GpuOperation
{
    public enum JoinType
    {
        INNER,
        LEFT,
    }

    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuJoinBridgeManager bridgeManager;
        private final int[] probeKeyChannels;
        private final int[] probeOutputChannels;
        private final JoinType joinType;
        private final List<Type> buildOutputTypes;
        private final boolean filteredJoin;

        private boolean closed;

        public Factory(
                GpuJoinBridgeManager bridgeManager,
                int[] probeKeyChannels,
                int[] probeOutputChannels,
                JoinType joinType,
                List<Type> buildOutputTypes,
                boolean filteredJoin)
        {
            this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
            this.probeKeyChannels = requireNonNull(probeKeyChannels, "probeKeyChannels is null").clone();
            this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null").clone();
            this.joinType = requireNonNull(joinType, "joinType is null");
            this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
            this.filteredJoin = filteredJoin;
        }

        @Override
        public Factory duplicate()
        {
            checkState(!closed, "Already closed");
            bridgeManager.probeOperatorFactoryDuplicated();
            return new Factory(bridgeManager, probeKeyChannels, probeOutputChannels, joinType, buildOutputTypes, filteredJoin);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuLookupJoin(
                    source,
                    bridgeManager,
                    probeKeyChannels,
                    probeOutputChannels,
                    joinType,
                    buildOutputTypes,
                    filteredJoin);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Already closed");
            closed = true;
            bridgeManager.probeOperatorFactoryClosed();
        }
    }

    private final UncheckedCloser closer = UncheckedCloser.create();
    private final GpuOperation source;
    private final ListenableFuture<GpuJoinBridge> bridgeFuture;
    private final int[] probeKeyChannels;
    private final int[] probeOutputChannels;
    private final JoinType joinType;
    private final List<Type> buildOutputTypes;
    private final boolean filteredJoin;

    private GpuLookupJoin(
            GpuOperation source,
            GpuJoinBridgeManager bridgeManager,
            int[] probeKeyChannels,
            int[] probeOutputChannels,
            JoinType joinType,
            List<Type> buildOutputTypes,
            boolean filteredJoin)
    {
        this.source = requireNonNull(source, "source is null");
        this.bridgeFuture = bridgeManager.getBridgeFuture();
        this.probeKeyChannels = probeKeyChannels;
        this.probeOutputChannels = probeOutputChannels;
        this.joinType = joinType;
        this.buildOutputTypes = buildOutputTypes;
        this.filteredJoin = filteredJoin;

        closer.register(source);
        closer.register(bridgeManager::probeOperatorClosed);
    }

    @Override
    public @Move Result execute()
    {
        // TODO short-circuit when probe side is empty

        if (!bridgeFuture.isDone()) {
            return new Blocked(asVoid(bridgeFuture));
        }
        GpuJoinBridge bridge = getDone(bridgeFuture);

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Finished finished -> finished;
            case Data(GpuPage page) -> {
                try (page) {
                    yield processProbePage(page, bridge)
                            .<Result>map(Data::new)
                            .orElseGet(Yielded::new);
                }
            }
        };
    }

    private @Move Optional<@Own GpuPage> processProbePage(@Borrow GpuPage probePage, GpuJoinBridge joinBridge)
    {
        boolean probeSideEmpty = probePage.positionCount() == 0;
        boolean buildSideEmpty = joinBridge instanceof EmptyBuildSide;

        switch (joinType) {
            case INNER -> {
                if (probeSideEmpty || buildSideEmpty) {
                    return Optional.empty();
                }
            }
            case LEFT -> {
                if (probeSideEmpty) {
                    return Optional.empty();
                }
                if (buildSideEmpty) {
                    return Optional.of(emitProbeRowsWithNullBuild(probePage));
                }
            }
        }

        try (Table probeKeyTable = toTable(probePage, probeKeyChannels)) {
            @Own GatherMap[] maps;
            if (!filteredJoin) {
                HashJoinBridge bridge = (HashJoinBridge) joinBridge;
                maps = switch (joinType) {
                    case INNER -> probeKeyTable.innerJoinGatherMaps(bridge.hashJoin());
                    case LEFT -> probeKeyTable.leftJoinGatherMaps(bridge.hashJoin());
                };
            }
            else {
                FilteredHashJoinBridge bridge = (FilteredHashJoinBridge) joinBridge;
                // The compiled AST references columns by their source-layout channel index, so
                // hand the kernel the full probe page (wrapped as a cuDF Table view) and the
                // full build source table; the kernel only reads columns the AST refers to.
                try (Table probeSourceTable = toTable(probePage)) {
                    maps = switch (joinType) {
                        case INNER -> Table.mixedInnerJoinGatherMaps(probeKeyTable, bridge.buildKeysTable(), probeSourceTable, bridge.buildSourceTable(), bridge.compiledFilter(), NullEquality.UNEQUAL);
                        case LEFT -> Table.mixedLeftJoinGatherMaps(probeKeyTable, bridge.buildKeysTable(), probeSourceTable, bridge.buildSourceTable(), bridge.compiledFilter(), NullEquality.UNEQUAL);
                    };
                }
            }
            try {
                verify(maps.length == 2, "Expected exactly 2 gather maps from join, got %s", maps.length);
                GatherMap probeGatherMap = maps[0];
                GatherMap buildGatherMap = maps[1];
                long outputRowCount = probeGatherMap.getRowCount();
                // TODO support joins that produce more than 2B rows
                checkState(outputRowCount < Integer.MAX_VALUE, "Join output exceeds Integer.MAX_VALUE rows: %s", outputRowCount);
                int rows = toIntExact(outputRowCount);
                if (rows == 0) {
                    return Optional.empty();
                }
                // cuDF Table rejects an empty column array; skip creating the probe
                // output table when there are no probe output columns (e.g. COUNT(*)).
                if (probeOutputChannels.length == 0) {
                    return Optional.of(assembleOutput(null, probeGatherMap, joinBridge.buildOutputTable(), buildGatherMap, rows));
                }
                try (Table probleTable = toTable(probePage, probeOutputChannels)) {
                    return Optional.of(assembleOutput(probleTable, probeGatherMap, joinBridge.buildOutputTable(), buildGatherMap, rows));
                }
            }
            finally {
                for (GatherMap map : maps) {
                    map.close();
                }
            }
        }
    }

    private @Move GpuPage emitProbeRowsWithNullBuild(@Borrow GpuPage probePage)
    {
        @Own Column[] outputColumns = new Column[probeOutputChannels.length + buildOutputTypes.size()];
        try {
            for (int i = 0; i < probeOutputChannels.length; i++) {
                outputColumns[i] = probePage.column(probeOutputChannels[i]).incRefCount();
            }
            for (int i = 0; i < buildOutputTypes.size(); i++) {
                Type type = buildOutputTypes.get(i);
                DType dType = GpuTypeConversion.toDType(type)
                        .orElseThrow(() -> new IllegalStateException("Build type not GPU convertible: " + type));
                try (Scalar nullScalar = Scalar.fromNull(dType)) {
                    outputColumns[probeOutputChannels.length + i] = new DeviceMemory(ColumnVector.fromScalar(nullScalar, probePage.positionCount()));
                }
            }
            return new GpuPage(probePage.positionCount(), outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private @Move GpuPage assembleOutput(
            @Nullable @Borrow Table probeTable,
            @Borrow GatherMap probeGatherMap,
            @Nullable @Borrow Table buildTable,
            @Borrow GatherMap buildGatherMap,
            int rows)
    {
        OutOfBoundsPolicy probePolicy = switch (joinType) {
            case INNER, LEFT -> OutOfBoundsPolicy.DONT_CHECK;
        };
        OutOfBoundsPolicy buildPolicy = switch (joinType) {
            case INNER -> OutOfBoundsPolicy.DONT_CHECK;
            case LEFT -> OutOfBoundsPolicy.NULLIFY;
        };

        @Own Table probeGathered = null;
        @Own Table buildGathered = null;
        try {
            if (probeTable != null) {
                try (ColumnView probeMapView = probeGatherMap.toColumnView(0, rows)) {
                    probeGathered = probeTable.gather(probeMapView, probePolicy);
                }
            }
            if (buildTable != null) {
                try (ColumnView buildMapView = buildGatherMap.toColumnView(0, rows)) {
                    buildGathered = buildTable.gather(buildMapView, buildPolicy);
                }
            }

            int probeOutColumns = (probeGathered != null) ? probeGathered.getNumberOfColumns() : 0;
            int buildOutColumns = (buildGathered != null) ? buildGathered.getNumberOfColumns() : 0;
            @Own Column[] outputColumns = new Column[probeOutColumns + buildOutColumns];
            try {
                for (int i = 0; i < probeOutColumns; i++) {
                    outputColumns[i] = new DeviceMemory(probeGathered.getColumn(i).incRefCount());
                }
                for (int j = 0; j < buildOutColumns; j++) {
                    outputColumns[probeOutColumns + j] = new DeviceMemory(buildGathered.getColumn(j).incRefCount());
                }
                return new GpuPage(rows, outputColumns);
            }
            finally {
                closeColumns(outputColumns);
            }
        }
        finally {
            if (probeGathered != null) {
                probeGathered.close();
            }
            if (buildGathered != null) {
                buildGathered.close();
            }
        }
    }

    @Override
    public void close()
    {
        closer.close();
    }
}
