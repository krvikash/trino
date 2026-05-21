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
    private final int[] groupByChannels;
    private final int[] mergeGroupByChannels;

    GpuGroupByAggregation(
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            int[] groupByChannels,
            boolean inputRaw,
            long compactionThresholdBytes,
            int inputColumnCount)
    {
        super(source, aggregates, inputRaw, compactionThresholdBytes, inputColumnCount);
        this.groupByChannels = requireNonNull(groupByChannels, "groupByChannels is null");
        this.mergeGroupByChannels = IntStream.range(0, groupByChannels.length).toArray();
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
