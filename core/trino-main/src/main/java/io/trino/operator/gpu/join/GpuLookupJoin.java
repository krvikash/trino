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
import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.OutOfBoundsPolicy;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.join.GpuJoinBridgeManager.GpuJoinBridge;
import io.trino.plugin.base.util.AutoCloseableCloser;
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
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.operator.gpu.GpuUtils.closeColumns;
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

        public Factory(
                GpuJoinBridgeManager bridgeManager,
                int[] probeKeyChannels,
                int[] probeOutputChannels,
                JoinType joinType,
                List<Type> buildOutputTypes)
        {
            this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
            this.probeKeyChannels = requireNonNull(probeKeyChannels, "probeKeyChannels is null").clone();
            this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null").clone();
            this.joinType = requireNonNull(joinType, "joinType is null");
            this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuLookupJoin(
                    source,
                    bridgeManager,
                    probeKeyChannels,
                    probeOutputChannels,
                    joinType,
                    buildOutputTypes);
        }

        @Override
        public void noMoreOperators()
        {
            bridgeManager.probeOperatorFactoryClosed();
        }
    }

    private final GpuOperation source;
    private final GpuJoinBridgeManager bridgeManager;
    private final ListenableFuture<GpuJoinBridge> bridgeFuture;
    private final int[] probeKeyChannels;
    private final int[] probeOutputChannels;
    private final JoinType joinType;
    private final List<Type> buildOutputTypes;

    private GpuLookupJoin(
            GpuOperation source,
            GpuJoinBridgeManager bridgeManager,
            int[] probeKeyChannels,
            int[] probeOutputChannels,
            JoinType joinType,
            List<Type> buildOutputTypes)
    {
        this.source = requireNonNull(source, "source is null");
        this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
        this.bridgeFuture = bridgeManager.getBridgeFuture();
        this.probeKeyChannels = probeKeyChannels;
        this.probeOutputChannels = probeOutputChannels;
        this.joinType = joinType;
        this.buildOutputTypes = buildOutputTypes;
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

    private Optional<@Move GpuPage> processProbePage(@Borrow GpuPage probePage, GpuJoinBridge bridge)
    {
        boolean probeSideEmpty = probePage.positionCount() == 0;
        HashJoin hashJoin = bridge.hashJoin();
        boolean buildSideEmpty = hashJoin == null;

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

        try (Table probeKeyTable = buildTableFromChannels(probePage, probeKeyChannels)) {
            @Own GatherMap[] maps = switch (joinType) {
                case INNER -> probeKeyTable.innerJoinGatherMaps(hashJoin);
                case LEFT -> probeKeyTable.leftJoinGatherMaps(hashJoin);
            };
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
                    return Optional.of(assembleOutput(null, probeGatherMap, bridge.buildOutputTable(), buildGatherMap, rows));
                }
                try (Table probleTable = buildTableFromChannels(probePage, probeOutputChannels)) {
                    return Optional.of(assembleOutput(probleTable, probeGatherMap, bridge.buildOutputTable(), buildGatherMap, rows));
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
                ColumnVector cv = ((DeviceMemory) probePage.column(probeOutputChannels[i])).columnVector();
                outputColumns[i] = new DeviceMemory(cv.incRefCount());
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

    private static @Own Table buildTableFromChannels(@Borrow GpuPage page, int[] channels)
    {
        ColumnVector[] selected = new ColumnVector[channels.length];
        for (int i = 0; i < channels.length; i++) {
            selected[i] = ((DeviceMemory) page.column(channels[i])).columnVector();
        }
        return new Table(selected);
    }

    @Override
    public void close()
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            closer.register(source);
            closer.register(bridgeManager::probeOperatorClosed);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
