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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.GpuUtils.closeColumns;
import static io.trino.operator.gpu.GpuUtils.concatenateAndClose;
import static java.util.Objects.requireNonNull;

/**
 * GPU aggregation with GROUP BY.
 */
final class GpuGroupByAggregation
        extends GpuAggregation
{
    private final int[] groupByChannels;

    GpuGroupByAggregation(
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            int[] groupByChannels,
            boolean inputRaw)
    {
        super(source, aggregates, inputRaw);
        this.groupByChannels = requireNonNull(groupByChannels, "groupByChannels is null");
    }

    @Override
    protected Optional<@Move GpuPage> computeAggregation()
    {
        if (totalBufferedRowCount == 0) {
            return Optional.empty();
        }
        return Optional.of(computeGroupBy());
    }

    private @Move GpuPage computeGroupBy()
    {
        checkState(!inputTables.isEmpty(), "Expected non-empty inputTables");

        try (@Own Table concatenated = concatenateAndClose(inputTables)) {
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

            Table.GroupByOperation groupBy = concatenated.groupBy(
                    GroupByOptions.builder().withIgnoreNullKeys(false).build(),
                    groupByChannels);

            try (@Own Table result = groupBy.aggregate(aggregations)) {
                return convertResultToGpuPage(result);
            }
        }
    }

    private @Move GpuPage convertResultToGpuPage(@Borrow Table result)
    {
        int columnCount = groupByChannels.length + aggregates.size();
        checkState(result.getNumberOfColumns() == columnCount, "Expected %s columns but got %s", columnCount, result.getNumberOfColumns());

        @Own Column[] outputColumns = new Column[columnCount];
        try {
            for (int i = 0; i < groupByChannels.length; i++) {
                outputColumns[i] = new DeviceMemory(result.getColumn(i).incRefCount());
            }
            for (int i = 0; i < aggregates.size(); i++) {
                int resultIndex = groupByChannels.length + i;
                @Borrow ColumnVector resultColumn = result.getColumn(resultIndex);
                GpuAggregateFunction aggregate = aggregates.get(i);
                outputColumns[resultIndex] = new DeviceMemory(aggregate.postProcessGroupByResult(resultColumn));
            }
            return new GpuPage((int) result.getRowCount(), outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }
}
