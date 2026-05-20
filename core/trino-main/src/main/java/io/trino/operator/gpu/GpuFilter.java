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
package io.trino.operator.gpu;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * GPU operation that filters and/or projects data.
 */
public class GpuFilter
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final CompiledExpression filter;

        public Factory(CompiledExpression filter)
        {
            this.filter = requireNonNull(filter, "filter is null");
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(filter);
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuFilter(source, filter);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuOperation source;
    private final CompiledExpression filter;

    public GpuFilter(
            GpuOperation source,
            CompiledExpression filter)
    {
        this.source = requireNonNull(source, "source is null");
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public @Move Result execute()
    {
        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Finished finished -> finished;
            case Yielded yielded -> yielded;
            case Data(GpuPage page) -> {
                try (page) {
                    yield processPage(page)
                            .<Result>map(Data::new)
                            .orElseGet(Yielded::new);
                }
            }
        };
    }

    private Optional<@Move GpuPage> processPage(@Borrow GpuPage input)
    {
        List<Integer> inputChannels = filter.inputChannels().getInputChannels();
        List<@Borrow ColumnVector> inputs = inputChannels.stream()
                .map(input::column)
                .map(DeviceMemory.class::cast)
                .map(DeviceMemory::columnVector)
                .collect(toImmutableList());

        try (ColumnVector mask = filter.expression().evaluate(input.positionCount(), inputs)) {
            try (Scalar sum = mask.sum(DType.INT32)) {
                int retained = sum.isValid() ? sum.getInt() : 0;
                if (retained == 0) {
                    return Optional.empty();
                }

                @Borrow Column[] filteredColumns = new Column[input.columnCount()];
                List<@Borrow ColumnVector> columnVectors = new ArrayList<>();
                int[] columnVectorToIndex = new int[input.columnCount()];
                for (int i = 0; i < input.columnCount(); i++) {
                    switch (input.column(i)) {
                        case DeviceMemory deviceMemory -> {
                            columnVectorToIndex[columnVectors.size()] = i;
                            columnVectors.add(deviceMemory.columnVector());
                        }
                        case Blocks _ -> {
                            throw new UnsupportedOperationException("Implement filtering of in memory blocks");
                        }
                    }
                }

                // This must hold, otherwise we would not be doing GPU evaluation
                checkState(!columnVectors.isEmpty(), "No column vectors found");

                try (Table table = new Table(columnVectors.toArray(ColumnVector[]::new));
                        Table filtered = table.filter(mask)) {
                    for (int i = 0; i < columnVectors.size(); i++) {
                        int columnIndex = columnVectorToIndex[i];
                        filteredColumns[columnIndex] = new DeviceMemory(filtered.getColumn(i));
                    }
                    return Optional.of(new GpuPage(retained, filteredColumns));
                }
            }
        }
    }

    @Override
    public void close()
    {
        source.close();
    }
}
