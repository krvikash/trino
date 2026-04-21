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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.GpuUtils.closeColumns;
import static io.trino.operator.gpu.GpuUtils.concatenateAndClose;

/**
 * GPU aggregation without GROUP BY (global aggregation).
 */
final class GpuGlobalAggregation
        extends GpuAggregation
{
    GpuGlobalAggregation(
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            boolean inputRaw)
    {
        super(source, aggregates, inputRaw);
    }

    @Override
    protected Optional<@Move GpuPage> computeAggregation()
    {
        if (totalBufferedRowCount == 0) {
            return Optional.of(createEmptyInputResult());
        }
        if (inputTables.isEmpty()) {
            return Optional.of(computeWithoutColumns());
        }
        return Optional.of(computeWithColumns());
    }

    private @Move GpuPage createEmptyInputResult()
    {
        @Own Column[] outputColumns = new Column[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                try (@Own Scalar scalar = aggregates.get(i).emptyResult()) {
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
     * intermediate state is passed as columns, so we'd go through {@link #computeWithColumns()}.
     */
    private @Move GpuPage computeWithoutColumns()
    {
        checkState(inputRaw, "Expected raw input when no columns are buffered");

        @Own Column[] outputColumns = new Column[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                GpuAggregateFunction aggregate = aggregates.get(i);
                checkState(aggregate.inputChannel().isEmpty(), "Aggregate %s requires input column but none were buffered", aggregate.getClass().getSimpleName());
                try (@Own Scalar scalar = aggregate.reduce(totalBufferedRowCount)) {
                    outputColumns[i] = new DeviceMemory(ColumnVector.fromScalar(scalar, 1));
                }
            }
            return new GpuPage(1, outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private @Move GpuPage computeWithColumns()
    {
        checkState(!inputTables.isEmpty(), "Expected non-empty inputTables");

        try (@Own Table concatenated = concatenateAndClose(inputTables)) {
            return reduce(concatenated);
        }
    }

    private @Move GpuPage reduce(@Borrow Table input)
    {
        @Own Column[] outputColumns = new Column[aggregates.size()];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                GpuAggregateFunction aggregate = aggregates.get(i);
                try (@Own Scalar scalar = reduce(input, aggregate)) {
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
        if (aggregate.inputChannel().isPresent()) {
            @Borrow ColumnVector inputColumn = input.getColumn(aggregate.inputChannel().getAsInt());
            return inputRaw
                    ? aggregate.reduce(inputColumn, totalBufferedRowCount)
                    : aggregate.mergeReduce(inputColumn, totalBufferedRowCount);
        }
        return aggregate.reduce(totalBufferedRowCount);
    }
}
