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
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.GpuOperation;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Base class for GPU aggregation operations.
 * <p>
 * Buffers input GpuPages until source is finished, then performs aggregation and returns result.
 * TODO: This approach is suboptimal and should be improved: https://starburstdata.atlassian.net/browse/ENG-10569
 *
 * @see GpuGlobalAggregation
 * @see GpuGroupByAggregation
 */
public abstract class GpuAggregation
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final List<GpuAggregateFunction> aggregates;
        private final int[] groupByChannels;
        private final List<Type> outputTypes;
        /**
         * True if input is raw data (SINGLE/PARTIAL steps), false if input is intermediate state (FINAL/INTERMEDIATE steps).
         */
        private final boolean inputRaw;

        public Factory(List<GpuAggregateFunction> aggregates, int[] groupByChannels, List<Type> groupByTypes, boolean inputRaw)
        {
            this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
            this.groupByChannels = groupByChannels.clone();
            this.inputRaw = inputRaw;

            ImmutableList.Builder<Type> outputTypesBuilder = ImmutableList.builder();
            outputTypesBuilder.addAll(groupByTypes);
            for (GpuAggregateFunction aggregate : aggregates) {
                outputTypesBuilder.add(aggregate.outputType());
            }
            this.outputTypes = outputTypesBuilder.build();
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            if (groupByChannels.length == 0) {
                return new GpuGlobalAggregation(source, aggregates, inputRaw);
            }
            return new GpuGroupByAggregation(source, aggregates, groupByChannels, inputRaw);
        }

        public List<Type> getOutputTypes()
        {
            return outputTypes;
        }
    }

    private final GpuOperation source;
    protected final List<GpuAggregateFunction> aggregates;
    protected final boolean inputRaw;

    protected final List<@Own Table> inputTables = new ArrayList<>();
    protected long totalBufferedRowCount;
    private @Nullable @Own GpuPage result;
    private boolean finished;

    protected GpuAggregation(
            GpuOperation source,
            List<GpuAggregateFunction> aggregates,
            boolean inputRaw)
    {
        this.source = requireNonNull(source, "source is null");
        this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
        this.inputRaw = inputRaw;
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
            return new Data(page);
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Data(GpuPage page) -> {
                try (page) {
                    bufferPage(page);
                }
                yield new Yielded();
            }
            case Finished() -> {
                Optional<GpuPage> aggregationResult = computeAggregation();
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
            return;
        }

        @Borrow ColumnVector[] tableColumns = new ColumnVector[columnCount];
        for (int channel = 0; channel < columnCount; channel++) {
            // Table constructor calls incRefCount(), so columns outlive the page
            tableColumns[channel] = ((DeviceMemory) page.column(channel)).columnVector();
        }

        inputTables.add(new Table(tableColumns));
    }

    /**
     * Compute the aggregation result.
     *
     * @return the result page, or empty if there's no output (e.g., GROUP BY with no input rows)
     */
    protected abstract Optional<@Move GpuPage> computeAggregation();

    @Override
    public void close()
    {
        source.close();
        inputTables.forEach(Table::close);
        inputTables.clear();
        if (result != null) {
            result.close();
            result = null;
        }
    }
}
