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
package io.trino.operator.torin.aggregation;

import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.operator.torin.GpuOperation;
import io.trino.operator.torin.memory.AllocatedMemory;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.plugin.base.torin.TablesList;
import io.trino.plugin.base.torin.UncheckedCloser;
import io.trino.spi.torin.GpuPage;
import io.trino.spi.torin.MemoryAmount;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.torin.GpuUtils.toTable;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Base class for GPU aggregation operations.
 * <p>
 * Uses lazy compaction: incoming pages are buffered until the byte threshold or the child row count
 * threshold is exceeded, at which point buffered tables are flushed (via subclass-specific
 * {@link #preAggregate}) and merged with any previously compacted result (via {@link #mergePreAggregated}).
 *
 * @see GpuGlobalAggregation
 * @see GpuGroupByAggregation
 */
public abstract class GpuAggregation
        implements GpuOperation
{
    // cuDF concatenate fails when total child rows across all tables exceeds INT32_MAX.
    private static final int CHILD_ROWS_THRESHOLD = Integer.MAX_VALUE;

    /// Rows a group-by aggregation emits for empty input.
    ///
    /// A multi-grouping-set aggregation (GROUPING SETS / ROLLUP / CUBE) can include the empty grouping
    /// set, which is a global aggregation and therefore has a result even over an empty input. Each such
    /// grouping set gets one row: null in every grouping key except `groupIdChannel`, which carries the
    /// grouping set id, plus the empty-input value of every aggregate. This mirrors
    /// [io.trino.operator.HashAggregationOperator]'s default output.
    ///
    /// @param groupIdChannel index of the groupId key among the aggregation's grouping keys
    /// @param groupingSetIds ids of the empty grouping sets, one output row each
    public record DefaultOutput(int groupIdChannel, List<Integer> groupingSetIds)
    {
        public DefaultOutput
        {
            groupingSetIds = ImmutableList.copyOf(requireNonNull(groupingSetIds, "groupingSetIds is null"));
            checkArgument(groupIdChannel >= 0, "groupIdChannel is negative: %s", groupIdChannel);
            checkArgument(!groupingSetIds.isEmpty(), "groupingSetIds is empty");
        }
    }

    public static class Factory
            implements GpuOperation.Factory
    {
        private final List<GpuAggregateFunction> aggregates;
        private final int[] groupByChannels;
        private final List<Type> groupByTypes;
        private final Optional<DefaultOutput> defaultOutput;
        /**
         * True if input is raw data (SINGLE/PARTIAL steps), false if input is intermediate state (FINAL/INTERMEDIATE steps).
         */
        private final boolean inputRaw;
        private final boolean outputPartial;
        // derived
        private final List<Type> outputTypes;
        private final long compactionThresholdBytes;
        private final int inputColumnCount;

        public Factory(
                List<GpuAggregateFunction> aggregates,
                int[] groupByChannels,
                List<Type> groupByTypes,
                Optional<DefaultOutput> defaultOutput,
                boolean inputRaw,
                boolean outputPartial,
                long compactionThresholdBytes,
                int inputColumnCount)
        {
            this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
            this.groupByChannels = groupByChannels.clone();
            this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
            this.defaultOutput = requireNonNull(defaultOutput, "defaultOutput is null");
            defaultOutput.ifPresent(output -> checkArgument(
                    output.groupIdChannel() < groupByChannels.length,
                    "groupIdChannel %s is not one of the %s grouping keys",
                    output.groupIdChannel(),
                    groupByChannels.length));
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;

            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();
            outputTypes.addAll(groupByTypes);
            for (GpuAggregateFunction aggregate : aggregates) {
                outputTypes.add(aggregate.outputType());
            }
            this.outputTypes = outputTypes.build();
            this.compactionThresholdBytes = compactionThresholdBytes;
            this.inputColumnCount = inputColumnCount;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(aggregates, groupByChannels, groupByTypes, defaultOutput, inputRaw, outputPartial, compactionThresholdBytes, inputColumnCount);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            if (groupByChannels.length == 0) {
                // A grouping-key-less aggregation is the empty grouping set itself, so it has no separate default output.
                return new GpuGlobalAggregation(context, source, aggregates, inputRaw, outputPartial, compactionThresholdBytes, inputColumnCount);
            }
            return new GpuGroupByAggregation(context, source, aggregates, groupByChannels, groupByTypes, defaultOutput, inputRaw, outputPartial, compactionThresholdBytes, inputColumnCount);
        }

        @Override
        public void noMoreOperators() {}

        public List<Type> getOutputTypes()
        {
            return outputTypes;
        }
    }

    protected final Context context;
    private final GpuOperation source;
    protected final List<GpuAggregateFunction> aggregates;
    protected final boolean inputRaw;
    private final boolean outputPartial;
    private final long compactionThresholdBytes;

    private final @Own TablesList inputTables = TablesList.create();
    private long inputTablesTotalBytes;
    private final ClosingRef<Table> compactedTable = ClosingRef.empty();
    private final ClosingRef<AllocatedMemory> allocated;
    private final int[] maxNestedInputRowCountPerColumn;
    private long totalBufferedRowCount;
    private boolean finished;

    protected GpuAggregation(
            Context context,
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            boolean inputRaw,
            boolean outputPartial,
            long compactionThresholdBytes,
            int inputColumnCount)
    {
        this.context = requireNonNull(context, "context is null");
        this.source = requireNonNull(source, "source is null");
        this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
        this.inputRaw = inputRaw;
        this.outputPartial = outputPartial;
        this.compactionThresholdBytes = compactionThresholdBytes;
        this.maxNestedInputRowCountPerColumn = new int[inputColumnCount];
        this.allocated = ClosingRef.own(context.taskMemoryContext().allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
    }

    @Override
    public @Move Result execute()
    {
        if (finished) {
            return new Finished();
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory memory, GpuPage page) -> {
                bufferPage(memory, page);
                Optional<Data> flushed = maybeFlushPartial();
                yield flushed.isPresent() ? flushed.get() : new Yielded();
            }
            case Finished() -> {
                finished = true;
                Optional<GpuPage> aggregationResult = finishAggregation();
                if (aggregationResult.isEmpty()) {
                    yield new Finished();
                }
                GpuPage result = aggregationResult.get();
                allocated.borrow().update(result.retainedMemory());
                yield new Data(allocated.take(), result);
            }
        };
    }

    private void bufferPage(@Move AllocatedMemory pageAllocation, @Move GpuPage incomingPage)
    {
        try (pageAllocation; var page = ClosingRef.own(incomingPage)) {
            totalBufferedRowCount += page.borrow().positionCount();

            if (page.borrow().columnCount() == 0) {
                // When no columns, only totalBufferedRowCount is tracked
                return;
            }

            try (ClosingRef<Table> inputTable = ClosingRef.own(toTable(page.borrow()))) {
                page.close();
                long tableBytes = inputTable.borrow().getDeviceMemorySize();
                int[] tableRows = maxRowsPerColumn(inputTable.borrow());

                // Compact before appending, so the working set flushed at once stays within the
                // threshold rather than growing to threshold + this page. The post-append compaction
                // below still flushes a page that already exceeds the threshold on its own.
                if (!inputTables.isEmpty()
                        && (inputTablesTotalBytes + tableBytes > compactionThresholdBytes
                        || anyColumnExceedsRowCountThreshold(tableRows))) {
                    compact();
                }

                allocated.borrow().transferFrom(pageAllocation);
                inputTables.add(inputTable.take());
                inputTablesTotalBytes += tableBytes;
                addRowCounts(tableRows);

                if (inputTablesTotalBytes >= compactionThresholdBytes) {
                    compact();
                }
            }
        }
    }

    private @Move Optional<Data> maybeFlushPartial()
    {
        if (!outputPartial || compactedTable.isEmpty()) {
            return Optional.empty();
        }
        long compactedBytes = compactedTable.borrow().getDeviceMemorySize();
        if (compactedBytes <= compactionThresholdBytes) {
            return Optional.empty();
        }

        @Own Optional<GpuPage> aggregationResult = finishAggregation();

        inputTablesTotalBytes = 0;
        Arrays.fill(maxNestedInputRowCountPerColumn, 0);
        totalBufferedRowCount = 0;

        if (aggregationResult.isEmpty()) {
            return Optional.empty();
        }
        try (ClosingRef<GpuPage> flushed = ClosingRef.own(aggregationResult.get());
                ClosingRef<AllocatedMemory> movedAllocation = ClosingRef.own(context.taskMemoryContext().allocate(getClass().getSimpleName(), MemoryAmount.ZERO))) {
            MemoryAmount pageMemory = flushed.borrow().retainedMemory();
            allocated.borrow().update(pageMemory); // nothing gets retained
            movedAllocation.borrow().transferFrom(allocated.borrow(), pageMemory);
            return Optional.of(new Data(movedAllocation.take(), flushed.take()));
        }
    }

    private void compact()
    {
        if (inputTables.isEmpty()) {
            return;
        }

        // Peak working set of the compaction: a subclass-specific combination of the resident data
        // (`before`), the concatenation copy, and the groupBy hash scratch (which scales with rows,
        // not data). See compactPeakReservationBytes.
        MemoryAmount before = allocated.borrow().amount();
        boolean multiInput = inputTables.borrow().size() > 1;
        long inputRows = 0;
        for (Table table : inputTables.borrow()) {
            inputRows += table.getRowCount();
        }
        if (!compactedTable.isEmpty()) {
            inputRows += compactedTable.borrow().getRowCount();
        }
        long reservationBytes = compactPeakReservationBytes(inputTables.borrow().getFirst() /* any */, multiInput, before.gpuDeviceBytes(), inputRows);
        allocated.borrow().update(MemoryAmount.gpuDevice(reservationBytes));

        try (ClosingRef<Table> preAggregated = ClosingRef.empty();
                TablesList toMerge = TablesList.create()) {
            try (Table concatenated = inputTables.concatenateAndClear()) {
                preAggregated.set(preAggregate(concatenated));
            }
            finally {
                inputTablesTotalBytes = 0;
                Arrays.fill(maxNestedInputRowCountPerColumn, 0);
            }

            if (!compactedTable.isEmpty()) {
                toMerge.add(compactedTable.take());
                toMerge.add(preAggregated.take());
                try (Table concatenated = toMerge.concatenateAndClear()) {
                    compactedTable.set(mergePreAggregated(concatenated));
                }
            }
            else {
                compactedTable.set(preAggregated.take());
            }
        }

        allocated.borrow().update(MemoryAmount.gpuDevice(compactedTable.borrow().getDeviceMemorySize()));
    }

    private Optional<GpuPage> finishAggregation()
    {
        compact();
        if (compactedTable.isEmpty()) {
            return finishAggregation(null, totalBufferedRowCount);
        }
        try (Table table = compactedTable.take()) {
            // TODO we might need to account for memory needed to calculate `GpuAggregateFunction.postProcessGroupByResult`
            return finishAggregation(table, totalBufferedRowCount);
        }
    }

    /**
     * Pre-aggregate buffered tables into an intermediate table.
     */
    protected abstract @Move Table preAggregate(@Borrow Table table);

    /**
     * GPU device bytes to reserve for the peak working set of {@link #compact}. The peak is the larger
     * of two independent transient costs — the concatenation copy (proportional to data) and the
     * groupBy hash scratch (proportional to row count, not data) — added to the resident data.
     *
     * @param sample a buffered input table, for inspecting key column types
     * @param multiInput whether more than one table is buffered (so concatenation copies)
     * @param dataBytes resident data (buffered + previously compacted) device bytes
     * @param rows total input rows about to be aggregated (buffered + previously compacted)
     */
    protected abstract long compactPeakReservationBytes(@Borrow Table sample, boolean multiInput, long dataBytes, long rows);

    /**
     * Merge previously pre-aggregated intermediate tables into one.
     */
    protected abstract @Move Table mergePreAggregated(@Borrow Table table);

    /**
     * Produce the final output page from the compacted intermediate table.
     * The table may be {@code null} when no columns were buffered (e.g., COUNT(*) with no input columns).
     * A zero row count means no input remains since the last partial flush, or no input arrived at all.
     * Subclasses supply the empty-input result when required, including for a non-null empty table.
     *
     * @return the result page, or empty if there's no output (e.g., a group-by whose rows were all flushed)
     */
    protected abstract @Move Optional<@Own GpuPage> finishAggregation(@Nullable @Borrow Table table, long totalBufferedRowCount);

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(source);
            closer.register(allocated); // release after data resources below are closed
            closer.register(inputTables);
            closer.register(compactedTable);
        }
    }

    private boolean anyColumnExceedsRowCountThreshold(int[] rowCounts)
    {
        for (int i = 0; i < maxNestedInputRowCountPerColumn.length; i++) {
            long newRowCount = (long) maxNestedInputRowCountPerColumn[i] + (long) rowCounts[i];
            if (newRowCount > CHILD_ROWS_THRESHOLD) {
                return true;
            }
        }
        return false;
    }

    private void addRowCounts(int[] rowCounts)
    {
        for (int i = 0; i < maxNestedInputRowCountPerColumn.length; i++) {
            maxNestedInputRowCountPerColumn[i] += rowCounts[i];
        }
    }

    /**
     * Returns the maximum row count at any nesting level for each top-level column.
     * This approximates cuDF's {@code bounds_and_type_check} in concatenate.cu, which checks
     * {@code total_row_count <= INT32_MAX} at each level after concatenation.
     */
    private static int[] maxRowsPerColumn(@Borrow Table table)
    {
        int[] rows = new int[table.getNumberOfColumns()];
        for (int columnIndex = 0; columnIndex < table.getNumberOfColumns(); columnIndex++) {
            rows[columnIndex] = maxRowCount(table.getColumn(columnIndex));
        }
        return rows;
    }

    private static int maxRowCount(@Borrow ColumnView column)
    {
        int max = toIntExact(column.getRowCount());
        if (column.getType().isNestedType()) {
            for (int i = 0; i < column.getNumChildren(); i++) {
                try (ColumnView child = column.getChildColumnView(i)) {
                    max = Math.max(max, maxRowCount(child));
                }
            }
        }
        return max;
    }
}
