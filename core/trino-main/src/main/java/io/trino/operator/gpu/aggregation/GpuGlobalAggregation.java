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
import ai.rapids.cudf.Scalar;
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

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.base.gpu.GpuUtils.toGpuPage;
import static java.lang.Math.toIntExact;

/**
 * GPU aggregation without GROUP BY (global aggregation).
 */
final class GpuGlobalAggregation
        extends GpuAggregation
{
    GpuGlobalAggregation(
            Context context,
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            boolean inputRaw,
            long compactionThresholdBytes,
            int inputColumnCount)
    {
        super(context, source, aggregates, inputRaw, compactionThresholdBytes, inputColumnCount);
    }

    @Override
    protected long compactPeakMultiplier(@Borrow Table sample, boolean multiInput)
    {
        // preAggregate emits one scalar per aggregate (negligible). Concat dominates: 2x for
        // multi-table, ~1x for a single-table no-op.
        return multiInput ? 2 : 1;
    }

    @Override
    protected @Move Table preAggregate(@Borrow Table table)
    {
        @Own ColumnVector[] outputColumns = new ColumnVector[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                GpuAggregateFunction aggregate = aggregates.get(i);
                try (Scalar scalar = reduce(table, aggregate)) {
                    outputColumns[i] = ColumnVector.fromScalar(scalar, 1);
                }
            }
            return new Table(outputColumns);
        }
        finally {
            for (ColumnVector column : outputColumns) {
                if (column != null) {
                    column.close();
                }
            }
        }
    }

    @Override
    protected @Move Table mergePreAggregated(@Borrow Table table)
    {
        int positionCount = toIntExact(table.getRowCount());
        @Own ColumnVector[] outputColumns = new ColumnVector[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                @Borrow ColumnVector column = table.getColumn(i);
                try (Scalar scalar = aggregates.get(i).mergeReduce(column, positionCount)) {
                    outputColumns[i] = ColumnVector.fromScalar(scalar, 1);
                }
            }
            return new Table(outputColumns);
        }
        finally {
            for (ColumnVector column : outputColumns) {
                if (column != null) {
                    column.close();
                }
            }
        }
    }

    @Override
    protected @Move Optional<@Own GpuPage> finishAggregation(@Nullable @Borrow Table table, long totalBufferedRowCount)
    {
        if (totalBufferedRowCount == 0) {
            checkState(table == null, "Expected no table when no rows were buffered");
            return Optional.of(createEmptyInputResult());
        }
        if (table == null) {
            // All aggregates are input-less (e.g., COUNT(*)) — no columns were buffered
            return Optional.of(computeWithoutColumns(totalBufferedRowCount));
        }
        return Optional.of(toGpuPage(table));
    }

    private @Move GpuPage createEmptyInputResult()
    {
        @Own Column[] outputColumns = new Column[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                try (Scalar scalar = aggregates.get(i).emptyResult()) {
                    outputColumns[i] = new DeviceMemory(ColumnVector.fromScalar(scalar, 1));
                }
            }
            return new GpuPage(1, outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    /**
     * Computes aggregation when no GPU columns are needed (e.g., COUNT(*)).
     * <p>
     * This is only valid for raw input (PARTIAL/SINGLE steps). For FINAL/INTERMEDIATE steps,
     * intermediate state is passed as columns.
     */
    private @Move GpuPage computeWithoutColumns(long totalBufferedRowCount)
    {
        checkState(inputRaw, "Expected raw input when no columns are buffered");

        @Own Column[] outputColumns = new Column[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                GpuAggregateFunction aggregate = aggregates.get(i);
                checkState(aggregate.inputChannel().isEmpty(), "Aggregate %s requires input column but none were buffered", aggregate.getClass().getSimpleName());
                try (Scalar scalar = aggregate.reduce(totalBufferedRowCount)) {
                    outputColumns[i] = new DeviceMemory(ColumnVector.fromScalar(scalar, 1));
                }
            }
            return new GpuPage(1, outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private @Move Scalar reduce(Table input, GpuAggregateFunction aggregate)
    {
        int positionCount = toIntExact(input.getRowCount());
        if (aggregate.inputChannel().isPresent()) {
            @Borrow ColumnVector inputColumn = input.getColumn(aggregate.inputChannel().getAsInt());
            return inputRaw
                    ? aggregate.reduce(inputColumn, positionCount)
                    : aggregate.mergeReduce(inputColumn, positionCount);
        }
        return aggregate.reduce(positionCount);
    }
}
