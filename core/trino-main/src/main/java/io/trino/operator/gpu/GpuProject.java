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
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static java.util.Objects.requireNonNull;

/**
 * GPU operation that filters and/or projects data.
 */
public class GpuProject
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final List<Projection> projections;

        public Factory(List<Projection> projections)
        {
            this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(projections);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuProject(context, source, projections);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuOperation source;
    private final List<Projection> projections;

    public GpuProject(
            Context context,
            GpuOperation source,
            List<Projection> projections)
    {
        requireNonNull(context, "context is null");
        this.source = requireNonNull(source, "source is null");
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
    }

    @Override
    public @Move Result execute()
    {
        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Finished finished -> finished;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (memory; page) {
                    yield new Data(AllocatedMemory.untracked(), processPage(page));
                }
            }
        };
    }

    private @Move GpuPage processPage(@Borrow GpuPage input)
    {
        @Own Column[] newColumns = new Column[projections.size()];
        try {
            for (int i = 0; i < projections.size(); i++) {
                Projection projection = projections.get(i);
                newColumns[i] = switch (projection) {
                    case Projection.PassThrough passThrough -> input.column(passThrough.sourceChannel()).incRefCount();
                    case Projection.Gpu(CompiledExpression expression) -> {
                        List<Integer> inputChannels = expression.inputChannels().getInputChannels();
                        List<@Borrow ColumnVector> inputs = inputChannels.stream()
                                .map(input::column)
                                .map(DeviceMemory.class::cast)
                                .map(DeviceMemory::columnVector)
                                .collect(toImmutableList());
                        yield new DeviceMemory(expression.expression().evaluate(input.positionCount(), inputs));
                    }
                };
            }
            return new GpuPage(input.positionCount(), newColumns);
        }
        finally {
            closeColumns(newColumns);
        }
    }

    @Override
    public void close()
    {
        source.close();
    }

    /**
     * Projection specification: either pass-through a source column or evaluate a GPU expression.
     */
    public sealed interface Projection
    {
        /**
         * Pass through a column from the source without modification.
         */
        record PassThrough(int sourceChannel)
                implements Projection
        {
            public PassThrough
            {
                checkArgument(sourceChannel >= 0, "Invalid sourceChannel: %s", sourceChannel);
            }
        }

        /**
         * Evaluate a GPU expression to produce the output column.
         */
        record Gpu(CompiledExpression expression)
                implements Projection
        {
            public Gpu
            {
                requireNonNull(expression, "expression is null");
            }
        }
    }
}
