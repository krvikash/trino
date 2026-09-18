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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.OutOfBoundsPolicy;
import ai.rapids.cudf.ReductionAggregation;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.join.GpuCrossJoinBuildSideSupplier.GpuCrossJoinBuildSide;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.operator.gpu.join.GpuJoinUtils.MAX_OUTPUT_CHUNK_BYTES;
import static io.trino.operator.gpu.join.GpuJoinUtils.TARGET_OUTPUT_CHUNK_BYTES;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static java.lang.Math.ceilDiv;
import static java.lang.Math.clamp;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// Probe side of a cross join. Emits the product of the current probe page and the build side in
/// bounded chunks, so the full product never has to be materialized at once.
public final class GpuCrossJoin
        implements GpuOperation
{
    /// Rows an output chunk may not exceed, on top of the byte limits. A cuDF column cannot hold more
    /// than [Integer#MAX_VALUE] rows, so a chunk of cheap rows still has to be bounded by their count.
    private static final int MAX_OUTPUT_CHUNK_ROWS = 10_000_000;
    /// Per output row, for the INT64 sequence of absolute output positions. The sequence is released
    /// before the gather, so it bounds the peak while the maps are derived, not the chunk itself.
    private static final long POSITION_SEQUENCE_BYTES_PER_ROW = Long.BYTES;
    /// Per output row, for one INT32 gather map derived from the position sequence. A map is built for
    /// each side that contributes output columns, and lives until that side has been gathered.
    private static final long GATHER_MAP_BYTES_PER_ROW = Integer.BYTES;
    /// Upper bound for what cuDF materializes on top of a gathered variable-width column: a
    /// (pointer, length) pair and an offset per output row. Some inputs take a cheaper path.
    private static final long VARIABLE_WIDTH_GATHER_BYTES_PER_ROW = 16 + Long.BYTES;

    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuCrossJoinBuildSideSupplier buildSideSupplier;
        private final int[] probeOutputChannels;

        private boolean closed;

        public Factory(GpuCrossJoinBuildSideSupplier buildSideSupplier, int[] probeOutputChannels)
        {
            this.buildSideSupplier = requireNonNull(buildSideSupplier, "buildSideSupplier is null");
            this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null").clone();
        }

        @Override
        public Factory duplicate()
        {
            checkState(!closed, "Already closed");
            buildSideSupplier.probeOperatorFactoryDuplicated();
            return new Factory(buildSideSupplier, probeOutputChannels);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            checkState(!closed, "Already closed");
            return new GpuCrossJoin(context, source, buildSideSupplier, probeOutputChannels);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Already closed");
            closed = true;
            buildSideSupplier.probeOperatorFactoryClosed();
        }
    }

    private final UncheckedCloser closer = UncheckedCloser.create();
    private final UncheckedCloser finish = UncheckedCloser.create();
    private final GpuTaskMemoryContext taskMemoryContext;
    private final GpuOperation source;
    private final int[] probeOutputChannels;

    private final ListenableFuture<GpuCrossJoinBuildSide> buildSideFuture;
    private boolean buildMeasured;
    private long buildBytes;
    private long buildAverageBytesPerRow;
    private long buildMaxBytesPerRow;

    private @Own @Nullable AllocatedMemory probeAllocation;
    private @Own @Nullable Table probeTable;
    private int probeRowCount;
    private long probeRowsToOutput;
    private long gatherMapBytesPerRow;
    private int outputChunkRows;
    /// Device memory a chunk's output row is reserved at, until the chunk's page gives its actual size.
    /// Exact when the product is emitted as one chunk, an estimate when it is sliced.
    private long reservationBytesPerRow;

    private long probeEmittedRows;

    private GpuCrossJoin(Context context, GpuOperation source, GpuCrossJoinBuildSideSupplier buildSideSupplier, int[] probeOutputChannels)
    {
        this.taskMemoryContext = context.taskMemoryContext();
        this.source = requireNonNull(source, "source is null");
        this.buildSideFuture = buildSideSupplier.getBuildSideFuture();
        this.probeOutputChannels = probeOutputChannels;

        closer.register(source);
        closer.register(this::releaseProbePage);
        closer.register(finish);
        finish.register(buildSideSupplier::probeOperatorFinished);
    }

    @Override
    public @Move Result execute()
    {
        if (!buildSideFuture.isDone()) {
            return new Blocked(asVoid(buildSideFuture));
        }
        GpuCrossJoinBuildSide buildSide = getDone(buildSideFuture);
        if (buildSide.rowCount() == 0) {
            // No pairing is possible, so the probe input does not have to be drained
            finish.close();
            return new Finished();
        }

        if (probeEmittedRows == probeRowsToOutput) {
            releaseProbePage();
            @Own Result sourceResult = source.execute();
            switch (sourceResult) {
                case Blocked blocked -> {
                    return blocked;
                }
                case Yielded yielded -> {
                    return yielded;
                }
                case Finished finished -> {
                    finish.close();
                    return finished;
                }
                case Data(AllocatedMemory memory, GpuPage page) -> {
                    try (memory; ClosingRef<GpuPage> probePage = ClosingRef.own(page)) {
                        startProbePage(buildSide, memory, probePage);
                    }
                    if (probeRowsToOutput == 0) {
                        return new Yielded();
                    }
                }
            }
        }
        return emitChunk(buildSide);
    }

    private void startProbePage(GpuCrossJoinBuildSide buildSide, @Borrow AllocatedMemory pageAllocation, ClosingRef<GpuPage> probePage)
    {
        if (probePage.borrow().positionCount() == 0) {
            return;
        }

        probeRowCount = probePage.borrow().positionCount();
        probeAllocation = taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO);
        probeAllocation.transferFrom(pageAllocation);
        // Nothing is retained when no probe column is selected: the page is closed below and the
        // reservation has to drop to zero with it
        long probeDeviceBytes = 0;
        if (probeOutputChannels.length > 0) {
            probeTable = toTable(probePage.borrow(), probeOutputChannels);
            probeDeviceBytes = probeTable.getDeviceMemorySize();
        }
        // Channels outside probeOutputChannels are released together with the page
        probePage.close();
        probeAllocation.update(MemoryAmount.gpuDevice(probeDeviceBytes));

        long buildRowCount = buildSide.rowCount();
        @Nullable @Borrow Table buildTable = buildSide.outputTable().orElse(null);
        if (!buildMeasured) {
            // Measured once per operator, unlike the per-page probe side
            buildMeasured = true;
            if (buildTable != null) {
                buildBytes = buildTable.getDeviceMemorySize();
                buildAverageBytesPerRow = ceilDiv(buildBytes, buildRowCount);
                buildMaxBytesPerRow = maxBytesPerRow(buildTable, toIntExact(buildRowCount));
            }
        }

        long probeAverageBytesPerRow = 0;
        long probeMaxBytesPerRow = 0;
        if (probeTable != null) {
            probeAverageBytesPerRow = ceilDiv(probeDeviceBytes, probeRowCount);
            probeMaxBytesPerRow = maxBytesPerRow(probeTable, probeRowCount);
        }

        int gatherMaps = ((probeTable == null) ? 0 : 1) + ((buildTable == null) ? 0 : 1);
        gatherMapBytesPerRow = gatherMaps * GATHER_MAP_BYTES_PER_ROW;
        long overheadBytesPerRow = gatherMapBytesPerRow;
        if (hasVariableWidthColumn(probeTable) || hasVariableWidthColumn(buildTable)) {
            // Probe and build columns are gathered one table at a time, so a single allowance covers both
            overheadBytesPerRow += VARIABLE_WIDTH_GATHER_BYTES_PER_ROW;
        }

        probeEmittedRows = 0;
        probeRowsToOutput = probeRowCount * buildRowCount;
        // Every probe row is repeated once per build row and every build row once per probe row, so the
        // product's size follows from the two input sizes without looking at individual rows
        long totalOutputBytes = saturatedAdd(
                saturatedAdd(
                        saturatedMultiply(probeDeviceBytes, buildRowCount),
                        saturatedMultiply(buildBytes, probeRowCount)),
                saturatedMultiply(probeRowsToOutput, overheadBytesPerRow));
        if (totalOutputBytes <= MAX_OUTPUT_CHUNK_BYTES) {
            outputChunkRows = clamp(probeRowsToOutput, 1, MAX_OUTPUT_CHUNK_ROWS);
            reservationBytesPerRow = ceilDiv(totalOutputBytes, probeRowsToOutput);
            return;
        }
        // A slice of the product holds an arbitrary run of rows, so its size is only estimated from the
        // page average. Aiming below the limit leaves room for rows wider than the average, and sizing
        // against the widest row of either side keeps even an all-wide slice within the limit.
        long averageBytesPerOutputRow = overheadBytesPerRow + probeAverageBytesPerRow + buildAverageBytesPerRow;
        long maxBytesPerOutputRow = overheadBytesPerRow + probeMaxBytesPerRow + buildMaxBytesPerRow;
        // Note: we reserve based on the mean of the average and the widest output row, so a chunk made
        // of rows wider than that mean is under-reserved until its page is materialized
        reservationBytesPerRow = ceilDiv(averageBytesPerOutputRow + maxBytesPerOutputRow, 2);
        outputChunkRows = clamp(
                min(TARGET_OUTPUT_CHUNK_BYTES / averageBytesPerOutputRow, MAX_OUTPUT_CHUNK_BYTES / maxBytesPerOutputRow),
                1,
                MAX_OUTPUT_CHUNK_ROWS);
    }

    /// Largest device footprint a single row of `table` contributes to a gathered output row. Rows of a
    /// variable-width column differ in width, so the widest is reduced out of them on the device.
    private long maxBytesPerRow(@Borrow Table table, int rowCount)
    {
        long maxBytesPerRow = 0;
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            ColumnVector column = table.getColumn(i);
            if (!column.getType().hasOffsets()) {
                // One byte covers the column's share of the validity mask
                maxBytesPerRow += column.getType().getSizeInBytes() + 1;
                continue;
            }
            maxBytesPerRow += Integer.BYTES + 1 + maxVariableWidthBytes(column, rowCount);
        }
        return maxBytesPerRow;
    }

    /// The byte counts are on the device only while the maximum is reduced out of them, so the
    /// reservation covers just that and only the single reduced value is kept.
    private long maxVariableWidthBytes(@Borrow ColumnVector column, int rowCount)
    {
        // The byte counts of one column, plus the validity mask copied along with them
        MemoryAmount byteCountsBytes = MemoryAmount.gpuDevice((long) rowCount * (Integer.BYTES + 1));
        try (AllocatedMemory _ = taskMemoryContext.allocate(getClass().getSimpleName(), byteCountsBytes);
                ColumnVector byteCounts = variableWidthByteCounts(column);
                Scalar maxByteCount = byteCounts.reduce(ReductionAggregation.max(), DType.INT32)) {
            // The reduction is invalid only when every row is null, and a null holds no bytes of its own
            return maxByteCount.isValid() ? maxByteCount.getInt() : 0;
        }
    }

    /// Bytes each row of a variable-width column holds. VARBINARY is the only [DType#LIST] the GPU
    /// produces and it is a list of single bytes, so its element count is its byte count.
    private static @Move ColumnVector variableWidthByteCounts(@Borrow ColumnVector column)
    {
        if (column.getType().equals(DType.STRING)) {
            return column.getByteCount();
        }
        verify(column.getType().equals(DType.LIST), "Unexpected variable-width type: %s", column.getType());
        try (ColumnView elements = column.getChildColumnView(0)) {
            verify(elements.getType().equals(DType.UINT8), "Unexpected list element type: %s", elements.getType());
        }
        return column.countElements();
    }

    private static boolean hasVariableWidthColumn(@Nullable @Borrow Table table)
    {
        if (table == null) {
            return false;
        }
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            if (table.getColumn(i).getType().hasOffsets()) {
                return true;
            }
        }
        return false;
    }

    private @Move Data emitChunk(GpuCrossJoinBuildSide buildSide)
    {
        int rows = toIntExact(min(outputChunkRows, probeRowsToOutput - probeEmittedRows));
        try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
                ClosingRef<GpuPage> outputPage = ClosingRef.own(buildChunk(buildSide, rows, allocation.borrow()))) {
            allocation.borrow().update(outputPage.borrow().retainedMemory());
            probeEmittedRows += rows;
            return new Data(allocation.take(), outputPage.take());
        }
    }

    /// Output row `emittedRows + i` pairs probe row `(emittedRows + i) / buildRowCount` with build
    /// row `(emittedRows + i) % buildRowCount`.
    private @Move GpuPage buildChunk(GpuCrossJoinBuildSide buildSide, int rows, @Borrow AllocatedMemory allocation)
    {
        @Nullable @Borrow Table probeSource = probeTable;
        @Nullable @Borrow Table buildSource = buildSide.outputTable().orElse(null);
        if (probeSource == null && buildSource == null) {
            return new GpuPage(rows, new Column[0]);
        }

        allocation.update(MemoryAmount.gpuDevice(rows * (POSITION_SEQUENCE_BYTES_PER_ROW + gatherMapBytesPerRow)));
        try (ClosingRef<Table> probeGathered = ClosingRef.empty();
                ClosingRef<Table> buildGathered = ClosingRef.empty()) {
            try (ClosingRef<ColumnVector> probeMap = ClosingRef.empty();
                    ClosingRef<ColumnVector> buildMap = ClosingRef.empty()) {
                try (Scalar start = Scalar.fromLong(probeEmittedRows);
                        Scalar step = Scalar.fromLong(1);
                        ColumnVector positions = ColumnVector.sequence(start, step, rows);
                        Scalar buildRows = Scalar.fromLong(buildSide.rowCount())) {
                    if (probeSource != null) {
                        probeMap.set(positions.binaryOp(BinaryOp.DIV, buildRows, DType.INT32));
                    }
                    if (buildSource != null) {
                        buildMap.set(positions.binaryOp(BinaryOp.MOD, buildRows, DType.INT32));
                    }
                }
                // The position sequence is released above, the gather maps live until the output columns are built
                allocation.update(MemoryAmount.gpuDevice(rows * reservationBytesPerRow));
                if (probeSource != null) {
                    probeGathered.set(probeSource.gather(probeMap.borrow(), OutOfBoundsPolicy.DONT_CHECK));
                }
                if (buildSource != null) {
                    buildGathered.set(buildSource.gather(buildMap.borrow(), OutOfBoundsPolicy.DONT_CHECK));
                }
            }
            return assembleOutput(
                    rows,
                    probeGathered.isEmpty() ? null : probeGathered.borrow(),
                    buildGathered.isEmpty() ? null : buildGathered.borrow());
        }
    }

    /// Either table is absent when its side contributes no output column; both are absent only for
    /// a `COUNT(*)`-shaped join, which [#buildChunk] answers without gathering.
    private static @Move GpuPage assembleOutput(int rows, @Nullable @Borrow Table probeGathered, @Nullable @Borrow Table buildGathered)
    {
        int probeColumns = (probeGathered == null) ? 0 : probeGathered.getNumberOfColumns();
        int buildColumns = (buildGathered == null) ? 0 : buildGathered.getNumberOfColumns();
        @Own Column[] outputColumns = new Column[probeColumns + buildColumns];
        try {
            for (int i = 0; i < probeColumns; i++) {
                outputColumns[i] = new DeviceMemory(probeGathered.getColumn(i).incRefCount());
            }
            for (int i = 0; i < buildColumns; i++) {
                outputColumns[probeColumns + i] = new DeviceMemory(buildGathered.getColumn(i).incRefCount());
            }
            return new GpuPage(rows, outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private void releaseProbePage()
    {
        try (var _ = probeAllocation;
                var _ = probeTable) {
            probeAllocation = null;
            probeTable = null;
            probeRowCount = 0;
            probeEmittedRows = 0;
            probeRowsToOutput = 0;
        }
    }

    @Override
    public void close()
    {
        closer.close();
    }
}
