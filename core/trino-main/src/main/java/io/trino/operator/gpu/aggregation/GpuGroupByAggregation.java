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

import ai.rapids.cudf.CloseableArray;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.GroupByAggregationOnColumn;
import ai.rapids.cudf.GroupByOptions;
import ai.rapids.cudf.Table;
import io.trino.operator.gpu.GpuOperation;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static java.util.Objects.requireNonNull;

/**
 * GPU aggregation with GROUP BY.
 */
final class GpuGroupByAggregation
        extends GpuAggregation
{
    // Fixed-width keys are narrow (e.g. an 8-byte BIGINT), so the 2x-data floor in
    // compactPeakReservationBytes does not cover the groupBy hash scratch; add a row-proportional term.
    // Calibrated so count-type aggregations (data ~16 B/row) reach the previously-validated ~3x data;
    // measured ~8-16 B/row of over-input working set for INT64 keys, so 32 keeps a margin.
    private static final long FIXED_WIDTH_SCRATCH_BYTES_PER_ROW = 32;

    private final int[] groupByChannels;
    private final int[] mergeGroupByChannels;

    GpuGroupByAggregation(
            Context context,
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            int[] groupByChannels,
            boolean inputRaw,
            boolean outputPartial,
            long compactionThresholdBytes,
            int inputColumnCount)
    {
        super(context, source, aggregates, inputRaw, outputPartial, compactionThresholdBytes, inputColumnCount);
        this.groupByChannels = requireNonNull(groupByChannels, "groupByChannels is null");
        this.mergeGroupByChannels = IntStream.range(0, groupByChannels.length).toArray();
    }

    @Override
    protected long compactPeakReservationBytes(@Borrow Table sample, boolean multiInput, long dataBytes, long rows)
    {
        // The compaction peak is the larger of two independent transients, not one multiplier on data:
        //   - concatenation copies the data once -> ~2x data;
        //   - the groupBy holds its input plus output plus hash scratch.
        // reserve max(2*data, data + hashScratch). This is tight for wide-state aggregations (data
        // dominates -> ~2x, vs. the previous flat 3x that over-reserved them, e.g. TPC-H q18's
        // `group by l_orderkey`) and still ~3x for narrow high-cardinality ones like count.
        //
        // hashScratch is a per-row term ONLY for fixed-width keys. For variable-width / nested keys the
        // wide per-row key data already makes the 2x-data floor cover the scratch, so we add nothing.
        // We deliberately do NOT add a per-row term for them: it would reserve for the all-distinct worst
        // case, but the reservation cannot know the group cardinality, so on a low-cardinality string
        // group-by (e.g. q01's returnflag/linestatus, ~12 groups) it would over-reserve and OOM under
        // concurrency. The trade-off is that a genuinely high-cardinality string key is under-reserved
        // (accepted; matches the previous behavior). Sizing this correctly needs NDV, which is not cheap.
        long hashScratchBytes = fixedWidthKeys(sample) ? FIXED_WIDTH_SCRATCH_BYTES_PER_ROW * rows : 0;
        return Math.max(2 * dataBytes, dataBytes + hashScratchBytes);
    }

    private boolean fixedWidthKeys(@Borrow Table sample)
    {
        int[] keyChannels = inputRaw ? groupByChannels : mergeGroupByChannels;
        for (int channel : keyChannels) {
            DType type = sample.getColumn(channel).getType();
            if (type.isNestedType() || type.getTypeId() == DType.DTypeEnum.STRING) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected @Move Table preAggregate(@Borrow Table table)
    {
        checkState(table.getNumberOfColumns() > 0, "GROUP BY aggregation requires at least one column");

        GroupByAggregationOnColumn[] aggregations = new GroupByAggregationOnColumn[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            GpuAggregateFunction aggregate = aggregates.get(i);
            // For aggregates with input channel, use that column; for COUNT(*), use first group-by column
            int aggInputColumn = aggregate.inputChannel().isPresent()
                    ? aggregate.inputChannel().getAsInt()
                    : 0;  // COUNT(*) doesn't use column values, any column works
            aggregations[i] = (inputRaw ? aggregate.groupByAggregation() : aggregate.mergeAggregation())
                    .onColumn(aggInputColumn);
        }

        try (Table raw = aggregate(table, groupByChannels, aggregations);
                CloseableArray<ColumnVector> normalized = CloseableArray.wrap(normalizeAggregateColumns(raw))) {
            return new Table(normalized.getArray());
        }
    }

    @Override
    protected @Move Table mergePreAggregated(@Borrow Table table)
    {
        GroupByAggregationOnColumn[] aggregations = new GroupByAggregationOnColumn[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            aggregations[i] = aggregates.get(i).mergeAggregation()
                    .onColumn(groupByChannels.length + i);
        }
        return aggregate(table, mergeGroupByChannels, aggregations);
    }

    private @Move Table aggregate(@Borrow Table table, int[] groupByChannels, GroupByAggregationOnColumn[] aggregations)
    {
        return table.groupBy(
                        GroupByOptions.builder().withIgnoreNullKeys(false).build(),
                        groupByChannels)
                .aggregate(aggregations);
    }

    @Override
    protected @Move Optional<@Own GpuPage> finishAggregation(@Nullable @Borrow Table table, long totalBufferedRowCount)
    {
        if (table == null) {
            return Optional.empty();
        }
        return Optional.of(convertResultToGpuPage(table));
    }

    private @Move GpuPage convertResultToGpuPage(@Borrow Table result)
    {
        int columnCount = groupByChannels.length + aggregates.size();
        checkState(result.getNumberOfColumns() == columnCount, "Expected %s columns but got %s", columnCount, result.getNumberOfColumns());

        @Own Column[] outputColumns = new Column[columnCount];
        try {
            @Own ColumnVector[] normalized = normalizeAggregateColumns(result);
            checkState(normalized.length == outputColumns.length, "Expected %s normalized columns but got %s", outputColumns.length, normalized.length);
            for (int i = 0; i < normalized.length; i++) {
                outputColumns[i] = new DeviceMemory(normalized[i]);
            }
            return new GpuPage((int) result.getRowCount(), outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private @Move ColumnVector[] normalizeAggregateColumns(@Borrow Table result)
    {
        try (CloseableArray<ColumnVector> outputColumns = CloseableArray.wrap(new ColumnVector[result.getNumberOfColumns()])) {
            for (int i = 0; i < groupByChannels.length; i++) {
                outputColumns.set(i, result.getColumn(i).incRefCount());
            }
            for (int i = 0; i < aggregates.size(); i++) {
                int resultIndex = groupByChannels.length + i;
                @Borrow ColumnVector resultColumn = result.getColumn(resultIndex);
                GpuAggregateFunction aggregate = aggregates.get(i);
                outputColumns.set(resultIndex, aggregate.postProcessGroupByResult(resultColumn));
            }
            return outputColumns.release();
        }
    }
}
