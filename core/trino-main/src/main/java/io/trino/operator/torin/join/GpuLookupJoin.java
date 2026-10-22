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
package io.trino.operator.torin.join;

import ai.rapids.cudf.BinaryOp;
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
import io.trino.operator.torin.GpuOperation;
import io.trino.operator.torin.join.GpuJoinBridge.EmptyBuildSide;
import io.trino.operator.torin.join.GpuJoinBridge.FilteredHashJoinBridge;
import io.trino.operator.torin.join.GpuJoinBridge.HashJoinBridge;
import io.trino.operator.torin.memory.AllocatedMemory;
import io.trino.operator.torin.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.plugin.base.torin.UncheckedCloser;
import io.trino.spi.torin.Column;
import io.trino.spi.torin.Column.DeviceMemory;
import io.trino.spi.torin.GpuPage;
import io.trino.spi.torin.GpuTypeConversion;
import io.trino.spi.torin.MemoryAmount;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.operator.torin.join.GpuJoinUtils.MAX_OUTPUT_CHUNK_BYTES;
import static io.trino.operator.torin.join.GpuJoinUtils.TARGET_OUTPUT_CHUNK_BYTES;
import static io.trino.operator.torin.join.GpuJoinUtils.maxBytesPerRow;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getDistinctIndicesGpuDeviceMemoryUsage;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getMixedInnerJoinGpuDeviceMemoryUsage;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getMixedLeftJoinGpuDeviceMemoryUsage;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getNullColumnMemoryUsage;
import static io.trino.plugin.base.torin.GpuUtils.closeColumns;
import static io.trino.plugin.base.torin.GpuUtils.toTable;
import static io.trino.spi.torin.GpuTypeConversion.nullScalar;
import static java.lang.Math.clamp;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// Probe side of a hash join. A probe page is joined once, and its output is gathered from the gather maps in
/// chunks of about [GpuJoinUtils#TARGET_OUTPUT_CHUNK_BYTES], one per [#execute].
public final class GpuLookupJoin
        implements GpuOperation
{
    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL;

        /// Whether build rows no probe row matched are emitted too, which needs a match mask and an outer drain.
        public boolean isBuildOuter()
        {
            return switch (this) {
                case INNER, LEFT -> false;
                case RIGHT, FULL -> true;
            };
        }
    }

    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuJoinBridgeManager bridgeManager;
        private final int[] probeKeyChannels;
        private final int[] probeOutputChannels;
        private final JoinType joinType;
        private final List<Type> buildOutputTypes;
        private final int maxOutputChunkRows;
        private final boolean filteredJoin;

        private boolean closed;

        public Factory(
                GpuJoinBridgeManager bridgeManager,
                int[] probeKeyChannels,
                int[] probeOutputChannels,
                JoinType joinType,
                List<Type> buildOutputTypes,
                int maxOutputChunkRows,
                boolean filteredJoin)
        {
            this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
            this.probeKeyChannels = requireNonNull(probeKeyChannels, "probeKeyChannels is null").clone();
            this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null").clone();
            this.joinType = requireNonNull(joinType, "joinType is null");
            checkArgument(joinType.isBuildOuter() == bridgeManager.hasOuterDrain(), "%s join with a bridge manager whose outer drain is %s", joinType, bridgeManager.hasOuterDrain());
            this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
            checkArgument(maxOutputChunkRows > 0, "maxOutputChunkRows must be positive: %s", maxOutputChunkRows);
            this.maxOutputChunkRows = maxOutputChunkRows;
            this.filteredJoin = filteredJoin;
        }

        @Override
        public Factory duplicate()
        {
            checkState(!closed, "Already closed");
            bridgeManager.probeOperatorFactoryDuplicated();
            return new Factory(bridgeManager, probeKeyChannels, probeOutputChannels, joinType, buildOutputTypes, maxOutputChunkRows, filteredJoin);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuLookupJoin(
                    context,
                    source,
                    bridgeManager,
                    probeKeyChannels,
                    probeOutputChannels,
                    joinType,
                    buildOutputTypes,
                    maxOutputChunkRows,
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
    private final UncheckedCloser finish = UncheckedCloser.create();
    private final GpuTaskMemoryContext taskMemoryContext;
    private final GpuOperation source;
    private final ListenableFuture<GpuJoinBridge> bridgeFuture;
    private final int[] probeKeyChannels;
    private final int[] probeOutputChannels;
    private final JoinType joinType;
    private final List<Type> buildOutputTypes;
    private final List<DType> buildOutputDTypes;
    private final int maxOutputChunkRows;
    private final boolean filteredJoin;

    private boolean complete;

    // The probe page being joined and its gather maps, held until its last chunk is emitted
    private final ClosingRef<GpuPage> probePage = ClosingRef.empty();
    private final ClosingRef<GatherMaps> gatherMaps = ClosingRef.empty();
    /// Reserves [#probePage] and [#gatherMaps].
    private final ClosingRef<AllocatedMemory> probeAllocation = ClosingRef.empty();
    /// Rows of [#gatherMaps], the join output of [#probePage].
    private long outputRows;
    /// First row of [#gatherMaps] not emitted yet.
    private long nextOutputRow;
    /// Average device bytes per output page row of [#probePage].
    private double outputPageBytesPerRow;
    /// Rows an output chunk of [#probePage] holds.
    private int chunkRows;

    private GpuLookupJoin(
            Context context,
            GpuOperation source,
            GpuJoinBridgeManager bridgeManager,
            int[] probeKeyChannels,
            int[] probeOutputChannels,
            JoinType joinType,
            List<Type> buildOutputTypes,
            int maxOutputChunkRows,
            boolean filteredJoin)
    {
        this.taskMemoryContext = context.taskMemoryContext();
        this.source = requireNonNull(source, "source is null");
        this.probeKeyChannels = probeKeyChannels;
        this.probeOutputChannels = probeOutputChannels;
        this.joinType = joinType;
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.buildOutputDTypes = buildOutputTypes.stream()
                .map(type -> GpuTypeConversion.toDType(type)
                        .orElseThrow(() -> new IllegalStateException("Build type not GPU convertible: " + type)))
                .collect(toImmutableList());
        this.maxOutputChunkRows = maxOutputChunkRows;
        this.filteredJoin = filteredJoin;
        closer.register(source);
        closer.register(finish);
        closer.register(probeAllocation); // release after the probe page and its gather maps
        closer.register(gatherMaps);
        closer.register(probePage);
        this.bridgeFuture = bridgeManager.acquireBridgeForProbe();

        finish.register(() -> bridgeManager.probeOperatorFinished(!complete));
    }

    @Override
    public @Move Result execute()
    {
        // TODO short-circuit when probe side is empty

        if (!bridgeFuture.isDone()) {
            return new Blocked(asVoid(bridgeFuture));
        }
        GpuJoinBridge bridge = getDone(bridgeFuture);

        if (!probePage.isEmpty()) {
            return emitNextChunk(bridge);
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Finished finished -> {
                complete = true;
                finish.close();
                yield finished;
            }
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
                        memory;
                        ClosingRef<GpuPage> gpuPage = ClosingRef.own(page)) {
                    allocation.borrow().transferFrom(memory);
                    Optional<GpuPage> joinPage = processProbePage(gpuPage, bridge, allocation);
                    if (joinPage.isPresent()) {
                        try (ClosingRef<GpuPage> resultPage = ClosingRef.own(joinPage.get())) {
                            gpuPage.close();
                            allocation.borrow().update(resultPage.borrow().retainedMemory());
                            yield new Data(allocation.take(), resultPage.take());
                        }
                    }
                }
                if (!probePage.isEmpty()) {
                    yield emitNextChunk(bridge);
                }
                yield new Yielded();
            }
        };
    }

    /// Returns the output of `page` when it is bounded by the page itself. Otherwise, joins `page` and, if
    /// the join produces any output, takes it over with its gather maps, to be emitted in chunks.
    private @Move Optional<@Own GpuPage> processProbePage(ClosingRef<GpuPage> page, GpuJoinBridge joinBridge, ClosingRef<AllocatedMemory> pageAllocation)
    {
        @Borrow GpuPage probe = page.borrow();
        @Borrow AllocatedMemory allocation = pageAllocation.borrow();
        boolean probeSideEmpty = probe.positionCount() == 0;
        boolean buildSideEmpty = joinBridge instanceof EmptyBuildSide;

        switch (joinType) {
            case INNER, RIGHT -> {
                if (probeSideEmpty || buildSideEmpty) {
                    return Optional.empty();
                }
            }
            case LEFT, FULL -> {
                if (probeSideEmpty) {
                    return Optional.empty();
                }
                if (buildSideEmpty) {
                    // Bounded by the probe page, as the probe columns are shared rather than gathered
                    return Optional.of(emitProbeRowsWithNullBuild(probe, allocation));
                }
            }
        }

        @Own GatherMap[] maps;
        MemoryAmount reservationBeforeJoin = allocation.amount();
        try (Table probeKeyTable = toTable(probe, probeKeyChannels)) {
            if (!filteredJoin) {
                HashJoinBridge bridge = (HashJoinBridge) joinBridge;
                long joinOutputRowCount = switch (joinType) {
                    case INNER, RIGHT -> probeKeyTable.innerJoinRowCount(bridge.hashJoin());
                    case LEFT, FULL -> probeKeyTable.leftJoinRowCount(bridge.hashJoin());
                };
                if (joinOutputRowCount == 0) {
                    return Optional.empty();
                }
                allocation.update(allocation.amount().add(estimateGatherMapsMemoryUsage(joinOutputRowCount)));
                maps = switch (joinType) {
                    case INNER, RIGHT -> probeKeyTable.innerJoinGatherMaps(bridge.hashJoin(), joinOutputRowCount);
                    case LEFT, FULL -> probeKeyTable.leftJoinGatherMaps(bridge.hashJoin(), joinOutputRowCount);
                };
            }
            else {
                FilteredHashJoinBridge bridge = (FilteredHashJoinBridge) joinBridge;
                // cuDF materializes every equality match before applying the filter, so the peak memory
                // of the mixed join scales with the equality match count rather than with row counts.
                long equalityMatchCount = switch (joinType) {
                    case INNER, RIGHT -> probeKeyTable.innerJoinRowCount(bridge.hashJoin());
                    case LEFT, FULL -> probeKeyTable.leftJoinRowCount(bridge.hashJoin());
                };
                if (equalityMatchCount == 0) {
                    // No equality match can survive the filter, so there is nothing to join
                    return Optional.empty();
                }
                // The compiled AST references columns by their source-layout channel index, so
                // hand the kernel the full probe page (wrapped as a cuDF Table view) and the
                // full build source table; the kernel only reads columns the AST refers to.
                try (Table probeSourceTable = toTable(probe)) {
                    MemoryAmount estimatedMemory = estimateFilteredJoinMemoryUsage(probeSourceTable, bridge, equalityMatchCount);
                    allocation.update(allocation.amount().add(estimatedMemory));
                    maps = switch (joinType) {
                        case INNER, RIGHT -> Table.mixedInnerJoinGatherMaps(probeKeyTable, bridge.buildKeysTable(), probeSourceTable, bridge.buildSourceTable(), bridge.compiledFilter(), NullEquality.UNEQUAL);
                        case LEFT, FULL -> Table.mixedLeftJoinGatherMaps(probeKeyTable, bridge.buildKeysTable(), probeSourceTable, bridge.buildSourceTable(), bridge.compiledFilter(), NullEquality.UNEQUAL);
                    };
                }
            }
        }
        try (ClosingRef<GatherMaps> joined = ClosingRef.own(GatherMaps.own(maps))) {
            long rows = joined.borrow().probe().getRowCount();
            if (rows == 0) {
                return Optional.empty();
            }
            if (filteredJoin) {
                // A filtered join's output size is only known once the join has run, so unlike the
                // non-filtered path it could not be reserved upfront. The mixed join's temporaries are
                // gone by now, so replace their reservation with one for the gather maps.
                allocation.update(reservationBeforeJoin.add(estimateGatherMapsMemoryUsage(rows)));
            }
            outputRows = rows;
            nextOutputRow = 0;
            outputPageBytesPerRow = outputPageBytesPerRow(probe, joinBridge);
            // A chunk is a contiguous run of the gather maps, so a probe row on a hot key can fill it with
            // its repeats. Sizing against the widest row keeps even an all-wide chunk within the limit.
            long maxOutputPageBytesPerRow = maxOutputPageBytesPerRow(probe, joinBridge);
            chunkRows = clamp(
                    min((long) (TARGET_OUTPUT_CHUNK_BYTES / outputPageBytesPerRow), MAX_OUTPUT_CHUNK_BYTES / maxOutputPageBytesPerRow),
                    1,
                    maxOutputChunkRows);
            gatherMaps.set(joined.take());
            probePage.set(page.take());
            probeAllocation.set(pageAllocation.take());
            return Optional.empty();
        }
    }

    /// Gathers the next chunk of the probe page's join output. The probe page is released with its last chunk.
    private @Move Result emitNextChunk(GpuJoinBridge joinBridge)
    {
        @Borrow GpuPage probe = probePage.borrow();
        @Borrow GatherMaps maps = gatherMaps.borrow();
        long start = nextOutputRow;
        int rows = toIntExact(min(chunkRows, outputRows - start));
        try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO))) {
            if (joinType.isBuildOuter()) {
                // Update matched rows eagerly to reduce memory usage
                markMatchedBuildRows(maps.build(), start, rows, joinBridge);
            }
            // Reserved after marking matched rows, as the page does not exist while they are marked
            allocation.borrow().update(estimateOutputPageMemoryUsage(rows));
            try (ClosingRef<GpuPage> resultPage = ClosingRef.own(gatherChunk(probe, maps, joinBridge, start, rows))) {
                nextOutputRow = start + rows;
                if (nextOutputRow == outputRows) {
                    releaseProbePage();
                }
                allocation.borrow().update(resultPage.borrow().retainedMemory());
                return new Data(allocation.take(), resultPage.take());
            }
        }
    }

    private @Move GpuPage gatherChunk(@Borrow GpuPage probe, @Borrow GatherMaps maps, GpuJoinBridge joinBridge, long start, int rows)
    {
        // cuDF Table rejects an empty column array; skip creating the probe
        // output table when there are no probe output columns (e.g. COUNT(*)).
        if (probeOutputChannels.length == 0) {
            return assembleOutput(null, maps.probe(), joinBridge.buildOutputTable(), maps.build(), start, rows);
        }
        try (Table probeTable = toTable(probe, probeOutputChannels)) {
            return assembleOutput(probeTable, maps.probe(), joinBridge.buildOutputTable(), maps.build(), start, rows);
        }
    }

    /// Average device bytes of an output page row of `probe`: the gathered probe and build output columns.
    private double outputPageBytesPerRow(@Borrow GpuPage probe, GpuJoinBridge joinBridge)
    {
        double bytesPerRow = 0;
        for (int channel : probeOutputChannels) {
            bytesPerRow += (double) probe.column(channel).retainedDeviceMemoryBytes() / probe.positionCount();
        }
        Table buildOutputTable = joinBridge.buildOutputTable();
        if (buildOutputTable != null && buildOutputTable.getRowCount() > 0) {
            bytesPerRow += (double) buildOutputTable.getDeviceMemorySize() / buildOutputTable.getRowCount();
        }
        return bytesPerRow;
    }

    /// Device bytes of the widest output page row of `probe`: its widest probe output row next to the widest build output row.
    private long maxOutputPageBytesPerRow(@Borrow GpuPage probe, GpuJoinBridge joinBridge)
    {
        long bytesPerRow = joinBridge.buildOutputMaxBytesPerRow();
        if (probeOutputChannels.length > 0) {
            try (Table probeTable = toTable(probe, probeOutputChannels)) {
                bytesPerRow += maxBytesPerRow(taskMemoryContext, getClass().getSimpleName(), probeTable, probe.positionCount());
            }
        }
        // Not 0, as it divides the chunk limit
        return Math.max(bytesPerRow, 1);
    }

    /// Estimates the probe and build gather maps a join of `outputRows` rows produces.
    private static MemoryAmount estimateGatherMapsMemoryUsage(long outputRows)
    {
        return MemoryAmount.gpuDevice(2L * outputRows * Integer.BYTES);
    }

    /// Estimates the output page gathered from `outputRows` rows of the gather maps.
    private MemoryAmount estimateOutputPageMemoryUsage(long outputRows)
    {
        return MemoryAmount.gpuDevice((long) Math.ceil(outputRows * outputPageBytesPerRow));
    }

    /// Estimates what the mixed join itself consumes. The output gather maps are not included; their
    /// size is only known once the join has run and is reserved separately.
    // TODO: https://github.com/NVIDIA/cudf/issues/22748 might let the output size be known upfront.
    private MemoryAmount estimateFilteredJoinMemoryUsage(@Borrow Table probeSourceTable, FilteredHashJoinBridge bridge, long equalityMatchCount)
    {
        long bytes = switch (joinType) {
            case INNER, RIGHT -> getMixedInnerJoinGpuDeviceMemoryUsage(bridge.buildKeysTable(), bridge.buildSourceTable(), probeSourceTable, equalityMatchCount);
            case LEFT, FULL -> getMixedLeftJoinGpuDeviceMemoryUsage(bridge.buildKeysTable(), bridge.buildSourceTable(), probeSourceTable, equalityMatchCount);
        };
        return MemoryAmount.gpuDevice(bytes);
    }

    private @Move GpuPage emitProbeRowsWithNullBuild(@Borrow GpuPage probePage, @Borrow AllocatedMemory allocation)
    {
        allocation.update(allocation.amount().add(estimateNullColumnMemoryUsage(probePage.positionCount())));
        @Own Column[] outputColumns = new Column[probeOutputChannels.length + buildOutputDTypes.size()];
        try {
            for (int i = 0; i < probeOutputChannels.length; i++) {
                outputColumns[i] = probePage.column(probeOutputChannels[i]).incRefCount();
            }
            for (int i = 0; i < buildOutputDTypes.size(); i++) {
                try (Scalar nullScalar = nullScalar(buildOutputTypes.get(i))) {
                    outputColumns[probeOutputChannels.length + i] = new DeviceMemory(ColumnVector.fromScalar(nullScalar, probePage.positionCount()));
                }
            }
            return new GpuPage(probePage.positionCount(), outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private MemoryAmount estimateNullColumnMemoryUsage(int positionCount)
    {
        long bytes = 0L;
        for (DType dType : buildOutputDTypes) {
            bytes += getNullColumnMemoryUsage(dType, positionCount);
        }
        return MemoryAmount.gpuDevice(bytes);
    }

    private @Move GpuPage assembleOutput(
            @Nullable @Borrow Table probeTable,
            @Borrow GatherMap probeGatherMap,
            @Nullable @Borrow Table buildTable,
            @Borrow GatherMap buildGatherMap,
            long start,
            int rows)
    {
        OutOfBoundsPolicy probePolicy = switch (joinType) {
            case INNER, LEFT, RIGHT, FULL -> OutOfBoundsPolicy.DONT_CHECK;
        };
        OutOfBoundsPolicy buildPolicy = switch (joinType) {
            case INNER, RIGHT -> OutOfBoundsPolicy.DONT_CHECK;
            case LEFT, FULL -> OutOfBoundsPolicy.NULLIFY;
        };

        @Own Table probeGathered = null;
        @Own Table buildGathered = null;
        try {
            if (probeTable != null) {
                try (ColumnView probeMapView = probeGatherMap.toColumnView(start, rows)) {
                    probeGathered = probeTable.gather(probeMapView, probePolicy);
                }
            }
            if (buildTable != null) {
                try (ColumnView buildMapView = buildGatherMap.toColumnView(start, rows)) {
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

    private void markMatchedBuildRows(@Borrow GatherMap buildGatherMap, long start, int rows, GpuJoinBridge bridge)
    {
        try (ClosingRef<AllocatedMemory> _ = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName() + "-distinct", MemoryAmount.gpuDevice(getDistinctIndicesGpuDeviceMemoryUsage(rows))));
                Table distinct = distinctBuildIndices(buildGatherMap, start, rows)) {
            switch (joinType) {
                case INNER, LEFT -> throw new IllegalStateException("No match mask for a %s join".formatted(joinType));
                // The probe runs an inner join, so every build index is a matched row
                case RIGHT -> bridge.matchMask().markMatched(distinct.getColumn(0));
                // The probe runs a left join, so a probe row with no match has an out-of-bounds build index (cuDF's
                // negative JoinNoMatch), and scattering it would be undefined. Deduplicated, it is at most one row, so
                // filter after the distinct. The copy behind the distinct is already freed, so the filter fits the
                // distinct's reservation.
                case FULL -> {
                    try (Scalar zero = Scalar.fromInt(0);
                            ColumnVector inBounds = distinct.getColumn(0).binaryOp(BinaryOp.GREATER_EQUAL, zero, DType.BOOL8);
                            Table matched = distinct.filter(inBounds)) {
                        if (matched.getRowCount() > 0) {
                            bridge.matchMask().markMatched(matched.getColumn(0));
                        }
                    }
                }
            }
        }
    }

    private static @Move Table distinctBuildIndices(@Borrow GatherMap buildGatherMap, long start, int rows)
    {
        try (ColumnView indices = buildGatherMap.toColumnView(start, rows);
                ColumnVector indicesCopy = indices.copyToColumnVector();
                Table indexTable = new Table(indicesCopy)) {
            return indexTable.dropDuplicates(new int[] {0}, Table.DuplicateKeepOption.KEEP_ANY, true);
        }
    }

    /// Taken rather than closed, as the refs are reused for the next probe page.
    private void releaseProbePage()
    {
        try (var _ = probeAllocation.take();
                var _ = gatherMaps.take();
                var _ = probePage.take()) {
            // closed in reverse order, the allocation last
        }
    }

    @Override
    public void close()
    {
        closer.close();
    }
}
