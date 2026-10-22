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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.OutOfBoundsPolicy;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.torin.GpuOperation;
import io.trino.operator.torin.memory.AllocatedMemory;
import io.trino.operator.torin.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.plugin.base.torin.UncheckedCloser;
import io.trino.spi.torin.Column;
import io.trino.spi.torin.Column.DeviceMemory;
import io.trino.spi.torin.GpuPage;
import io.trino.spi.torin.GpuTypeConversion;
import io.trino.spi.torin.MemoryAmount;
import io.trino.spi.torin.RuntimeCloseable;
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
import static io.trino.operator.torin.join.GpuJoinUtils.TARGET_OUTPUT_CHUNK_BYTES;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getNullColumnMemoryUsage;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getUnmatchedBuildIndicesGpuDeviceMemoryUsage;
import static io.trino.plugin.base.torin.GpuUtils.closeColumns;
import static io.trino.spi.torin.GpuTypeConversion.nullScalar;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// Head of the Outer join's driver: one task-lifecycle driver regardless of how many probe splits exist.
/// Waits for the build side and every probe, then inverts the shared match mask to get the never-matched build
/// rows and emits them with NULL probe columns, one slice per {@link #execute}. Never pulls its inert source.
public final class GpuJoinOuterDrain
        implements GpuOperation
{
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuJoinBridgeManager bridgeManager;
        private final List<Type> probeOutputTypes;

        private boolean closed;

        public Factory(GpuJoinBridgeManager bridgeManager, List<Type> probeOutputTypes)
        {
            this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
            checkArgument(bridgeManager.hasOuterDrain(), "Bridge manager has no outer drain");
            this.probeOutputTypes = ImmutableList.copyOf(requireNonNull(probeOutputTypes, "probeOutputTypes is null"));
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            checkState(!closed, "Already closed");
            return new GpuJoinOuterDrain(context, source, bridgeManager, probeOutputTypes);
        }

        @Override
        public Factory duplicate()
        {
            // Every drain emits all unmatched build rows, so a second one would duplicate them
            throw new UnsupportedOperationException("Outer drain factory cannot be duplicated");
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Already closed");
            closed = true;
            bridgeManager.outerDrainFactoryClosed();
        }
    }

    private final UncheckedCloser closer = UncheckedCloser.create();
    private final UncheckedCloser finish = UncheckedCloser.create();
    private final GpuTaskMemoryContext taskMemoryContext;
    private final GpuJoinBridgeManager bridgeManager;
    private final ListenableFuture<GpuJoinBridge> bridgeFuture;
    private final List<Type> probeOutputTypes;
    private final List<DType> probeOutputDTypes;

    /// The never-matched build-row indices (INT32) and the reservation for them.
    private record UnmatchedBuildRows(@Own AllocatedMemory memory, @Own ColumnVector indices)
            implements RuntimeCloseable
    {
        private UnmatchedBuildRows
        {
            requireNonNull(memory, "memory is null");
            requireNonNull(indices, "indices is null");
        }

        public int rows()
        {
            return toIntExact(indices.getRowCount());
        }

        public @Move ColumnView sliceView(int start, int count)
        {
            return ColumnView.fromDeviceBuffer(indices.getData(), (long) start * Integer.BYTES, DType.INT32, count);
        }

        @Override
        public void close()
        {
            indices.close();
            memory.close();
        }
    }

    // Set once on DRAINING -> COMPUTED, held across the slicing execute() calls
    private Optional<GpuJoinBridge> bridge = Optional.empty();
    private Optional<@Own UnmatchedBuildRows> unmatchedBuildRows = Optional.empty();
    private int drainCursor;
    private int unmatchedRowsPerSlice;

    public GpuJoinOuterDrain(GpuOperation.Context context, GpuOperation source, GpuJoinBridgeManager bridgeManager, List<Type> probeOutputTypes)
    {
        this.taskMemoryContext = context.taskMemoryContext();
        this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
        this.probeOutputTypes = ImmutableList.copyOf(requireNonNull(probeOutputTypes, "probeOutputTypes is null"));
        this.probeOutputDTypes = this.probeOutputTypes.stream()
                .map(type -> GpuTypeConversion.toDType(type)
                        .orElseThrow(() -> new IllegalStateException("Probe type not GPU convertible: " + type)))
                .collect(toImmutableList());
        closer.register(requireNonNull(source, "source is null"));
        closer.register(finish);
        this.bridgeFuture = bridgeManager.acquireBridgeForOuterDrain();
        finish.register(bridgeManager::outerDrainOperatorFinished);
        closer.register(this::releaseDrainState);
    }

    @Override
    public @Move Result execute()
    {
        if (!bridgeFuture.isDone()) {
            return new Blocked(asVoid(bridgeFuture));
        }
        ListenableFuture<Void> allProbesFinished = bridgeManager.whenAllProbesFinished();
        if (!allProbesFinished.isDone()) {
            return new Blocked(allProbesFinished);
        }
        if (bridgeManager.isMatchMaskIncomplete()) {
            finish.close();
            return new Finished();
        }
        if (bridge.isEmpty()) {
            bridge = Optional.ofNullable(getDone(bridgeFuture));
            computeUnmatchedBuildRows(bridge.orElseThrow());
            return new Yielded();
        }
        if (drainCursor == unmatchedBuildRows.map(UnmatchedBuildRows::rows).orElse(0)) {
            releaseDrainState();
            finish.close();
            return new Finished();
        }
        return emitNextSlice();
    }

    private void computeUnmatchedBuildRows(GpuJoinBridge bridge)
    {
        this.bridge = Optional.of(bridge);
        int buildRowCount = bridge.buildRowCount();
        if (buildRowCount == 0) {
            return;
        }
        GpuJoinMatchMask matchMask = requireNonNull(bridge.matchMask(), "matchMask is null");
        try (ClosingRef<AllocatedMemory> memory = ClosingRef.own(taskMemoryContext.allocate(
                getClass().getSimpleName() + "-unmatched",
                MemoryAmount.gpuDevice(getUnmatchedBuildIndicesGpuDeviceMemoryUsage(buildRowCount))));
                ClosingRef<ColumnVector> unmatched = ClosingRef.own(matchMask.unmatchedBuildIndices())) {
            if (unmatched.borrow().getRowCount() == 0) {
                return;
            }
            memory.borrow().update(MemoryAmount.gpuDevice(unmatched.borrow().getDeviceMemorySize()));
            unmatchedBuildRows = Optional.of(new UnmatchedBuildRows(memory.take(), unmatched.take()));
            unmatchedRowsPerSlice = targetRowsPerSlice(bridge);
        }
    }

    private @Move Result emitNextSlice()
    {
        UnmatchedBuildRows unmatched = unmatchedBuildRows.orElseThrow();
        int start = drainCursor;
        int count = Math.min(unmatchedRowsPerSlice, unmatched.rows() - start);
        try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName() + "-slice", MemoryAmount.ZERO));
                ColumnView sliceIndices = unmatched.sliceView(start, count)) {
            Table buildOutput = bridge.orElseThrow().buildOutputTable();
            @Own GpuPage page;
            if (buildOutput != null) {
                if (buildOutput.getRowCount() > 0) {
                    long estimatedGatherBytes = (long) Math.ceil(buildOutput.getDeviceMemorySize() * ((double) count / buildOutput.getRowCount()));
                    allocation.borrow().update(MemoryAmount.gpuDevice(estimatedGatherBytes));
                }
                try (Table unmatchedBuild = buildOutput.gather(sliceIndices, OutOfBoundsPolicy.DONT_CHECK)) {
                    page = emitBuildRowsWithNullProbe(unmatchedBuild, count, allocation.borrow());
                }
            }
            else {
                page = emitBuildRowsWithNullProbe(null, count, allocation.borrow());
            }
            try (ClosingRef<GpuPage> resultPage = ClosingRef.own(page)) {
                allocation.borrow().update(resultPage.borrow().retainedMemory());
                drainCursor = start + count;
                return new Data(allocation.take(), resultPage.take());
            }
        }
    }

    /// Sizes a slice to stay near {@link GpuJoinUtils#TARGET_OUTPUT_CHUNK_BYTES}, counting the NULL probe columns too.
    private int targetRowsPerSlice(GpuJoinBridge bridge)
    {
        Table buildOutput = bridge.buildOutputTable();
        long buildBytesPerRow = 0;
        if (buildOutput != null && buildOutput.getRowCount() > 0) {
            buildBytesPerRow = buildOutput.getDeviceMemorySize() / buildOutput.getRowCount();
        }
        long bytesPerRow = buildBytesPerRow + nullProbeBytesPerRow();
        if (bytesPerRow == 0) {
            return Integer.MAX_VALUE;
        }
        return toIntExact(Math.clamp(TARGET_OUTPUT_CHUNK_BYTES / bytesPerRow, 1, Integer.MAX_VALUE));
    }

    private long nullProbeBytesPerRow()
    {
        long bytesPerRow = 0;
        for (DType dType : probeOutputDTypes) {
            int sizeInBytes = dType.getSizeInBytes();
            // sizeInBytes == 0 marks a variable-width type: count its INT32 offset entry
            bytesPerRow += (sizeInBytes == 0) ? Integer.BYTES : sizeInBytes;
        }
        return bytesPerRow;
    }

    private void releaseDrainState()
    {
        unmatchedBuildRows.ifPresent(UnmatchedBuildRows::close);
        unmatchedBuildRows = Optional.empty();
    }

    private @Move GpuPage emitBuildRowsWithNullProbe(@Nullable @Borrow Table unmatchedBuild, int rows, @Borrow AllocatedMemory allocation)
    {
        int buildOutColumns = (unmatchedBuild != null) ? unmatchedBuild.getNumberOfColumns() : 0;
        allocation.update(allocation.amount().add(estimateNullProbeColumnMemoryUsage(rows)));
        @Own Column[] outputColumns = new Column[probeOutputTypes.size() + buildOutColumns];
        try {
            for (int i = 0; i < probeOutputTypes.size(); i++) {
                try (Scalar nullScalar = nullScalar(probeOutputTypes.get(i))) {
                    outputColumns[i] = new DeviceMemory(ColumnVector.fromScalar(nullScalar, rows));
                }
            }
            for (int j = 0; j < buildOutColumns; j++) {
                outputColumns[probeOutputTypes.size() + j] = new DeviceMemory(unmatchedBuild.getColumn(j).incRefCount());
            }
            return new GpuPage(rows, outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private MemoryAmount estimateNullProbeColumnMemoryUsage(int positionCount)
    {
        long bytes = 0L;
        for (DType dType : probeOutputDTypes) {
            bytes += getNullColumnMemoryUsage(dType, positionCount);
        }
        return MemoryAmount.gpuDevice(bytes);
    }

    @Override
    public void close()
    {
        closer.close();
    }
}
