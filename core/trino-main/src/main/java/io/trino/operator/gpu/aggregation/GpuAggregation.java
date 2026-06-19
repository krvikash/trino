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
package io.trino.operator.gpu.aggregation;

import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.TablesList;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.gpu.GpuUtils.toTable;
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

    public static class Factory
            implements GpuOperation.Factory
    {
        private final List<GpuAggregateFunction> aggregates;
        private final int[] groupByChannels;
        private final List<Type> groupByTypes;
        /**
         * True if input is raw data (SINGLE/PARTIAL steps), false if input is intermediate state (FINAL/INTERMEDIATE steps).
         */
        private final boolean inputRaw;
        // derived
        private final List<Type> outputTypes;
        private final long compactionThresholdBytes;
        private final int inputColumnCount;

        public Factory(List<GpuAggregateFunction> aggregates, int[] groupByChannels, List<Type> groupByTypes, boolean inputRaw, long compactionThresholdBytes, int inputColumnCount)
        {
            this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
            this.groupByChannels = groupByChannels.clone();
            this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
            this.inputRaw = inputRaw;

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
            return new Factory(aggregates, groupByChannels, groupByTypes, inputRaw, compactionThresholdBytes, inputColumnCount);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            if (groupByChannels.length == 0) {
                return new GpuGlobalAggregation(context, source, aggregates, inputRaw, compactionThresholdBytes, inputColumnCount);
            }
            return new GpuGroupByAggregation(context, source, aggregates, groupByChannels, inputRaw, compactionThresholdBytes, inputColumnCount);
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
    private final long compactionThresholdBytes;

    private final @Own TablesList inputTables = TablesList.create();
    private final ClosingRef<Table> compactedTable = ClosingRef.empty();
    private long totalInputBytes;
    private final int[] maxNestedInputRowCountPerColumn;
    private long totalBufferedRowCount;
    private @Nullable @Own GpuPage result;
    private boolean finished;

    protected GpuAggregation(
            Context context,
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            boolean inputRaw,
            long compactionThresholdBytes,
            int inputColumnCount)
    {
        this.context = requireNonNull(context, "context is null");
        this.source = requireNonNull(source, "source is null");
        this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
        this.inputRaw = inputRaw;
        this.compactionThresholdBytes = compactionThresholdBytes;
        this.maxNestedInputRowCountPerColumn = new int[inputColumnCount];
    }

    @Override
    public @Move Result execute()
    {
        if (finished) {
            return new Finished();
        }

        if (result != null) {
            GpuPage page = result;
            result = null;
            finished = true;
            return new Data(AllocatedMemory.untracked(), page);
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (memory; page) {
                    bufferPage(page);
                }
                yield new Yielded();
            }
            case Finished() -> {
                Optional<GpuPage> aggregationResult = finishAggregation();
                if (aggregationResult.isEmpty()) {
                    finished = true;
                    yield new Finished();
                }
                result = aggregationResult.get();
                yield new Yielded();  // Will return result on next execute()
            }
        };
    }

    private void bufferPage(@Borrow GpuPage page)
    {
        totalBufferedRowCount += page.positionCount();

        int columnCount = page.columnCount();
        if (columnCount == 0) {
            // TODO shouldn't we rather check position count?
            return;
        }

        try (ClosingRef<Table> inputTable = ClosingRef.own(toTable(page))) {
            long tableBytes = inputTable.borrow().getDeviceMemorySize();
            int[] tableRows = maxRowsPerColumn(inputTable.borrow());

            if (!inputTables.isEmpty()
                    && (totalInputBytes + tableBytes > compactionThresholdBytes
                    || anyColumnExceedsRowCountThreshold(tableRows))) {
                compact();
            }

            inputTables.add(inputTable.take());
            totalInputBytes += tableBytes;
            addRowCounts(tableRows);
        }
    }

    private void compact()
    {
        if (inputTables.isEmpty()) {
            return;
        }

        try (ClosingRef<Table> preAggregated = ClosingRef.empty();
                TablesList toMerge = TablesList.create()) {
            try (Table concatenated = inputTables.concatenateAndClear()) {
                preAggregated.set(preAggregate(concatenated));
            }
            finally {
                totalInputBytes = 0;
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
    }

    private Optional<GpuPage> finishAggregation()
    {
        compact();
        try (compactedTable) {
            if (compactedTable.isEmpty()) {
                return finishAggregation(null, totalBufferedRowCount);
            }
            return finishAggregation(compactedTable.borrow(), totalBufferedRowCount);
        }
    }

    /**
     * Pre-aggregate buffered tables into an intermediate table.
     */
    protected abstract @Move Table preAggregate(@Borrow Table table);

    /**
     * Merge previously pre-aggregated intermediate tables into one.
     */
    protected abstract @Move Table mergePreAggregated(@Borrow Table table);

    /**
     * Produce the final output page from the compacted intermediate table.
     * The table may be {@code null} when no columns were buffered (e.g., COUNT(*) with no input columns).
     *
     * @return the result page, or empty if there's no output (e.g., GROUP BY with no input rows)
     */
    protected abstract @Move Optional<@Own GpuPage> finishAggregation(@Nullable @Borrow Table table, long totalBufferedRowCount);

    @Override
    public void close()
    {
        source.close();
        inputTables.close();
        compactedTable.close();
        if (result != null) {
            result.close();
            result = null;
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
