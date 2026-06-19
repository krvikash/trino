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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import io.trino.operator.gpu.GpuDynamicFilterProvider.CompiledDynamicFilter;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.gpu.GpuUtils.toGpuPage;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static java.util.Objects.requireNonNull;

public class GpuFilter
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final Optional<CompiledExpression> staticFilter;
        private final Optional<GpuDynamicFilterProvider> dynamicFilter;
        private final OptionalDouble passThroughThreshold;

        public Factory(
                Optional<CompiledExpression> staticFilter,
                Optional<GpuDynamicFilterProvider> dynamicFilter,
                OptionalDouble passThroughThreshold)
        {
            checkArgument(staticFilter.isPresent() || dynamicFilter.isPresent(), "Either staticFilter or dynamicFilter must be present");
            checkArgument(staticFilter.isEmpty() || passThroughThreshold.isEmpty(), "staticFilter and passThroughThreshold cannot be both present: %s, %s", staticFilter, passThroughThreshold);
            this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.passThroughThreshold = requireNonNull(passThroughThreshold, "passThroughThreshold is null");
        }

        @Override
        public Factory duplicate()
        {
            dynamicFilter.ifPresent(GpuDynamicFilterProvider::operatorFactoryDuplicated);
            return new Factory(staticFilter, dynamicFilter, passThroughThreshold);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            dynamicFilter.ifPresent(GpuDynamicFilterProvider::operatorCreated);
            return new GpuFilter(context, source, staticFilter, dynamicFilter, passThroughThreshold);
        }

        @Override
        public void noMoreOperators()
        {
            dynamicFilter.ifPresent(GpuDynamicFilterProvider::noMoreOperators);
        }
    }

    private final GpuOperation source;
    private final Optional<CompiledExpression> staticFilter;
    private final Optional<GpuDynamicFilterProvider> dynamicFilter;
    private final OptionalDouble passThroughThreshold;

    private GpuFilter(
            Context context,
            GpuOperation source,
            Optional<CompiledExpression> staticFilter,
            Optional<GpuDynamicFilterProvider> dynamicFilter,
            OptionalDouble passThroughThreshold)
    {
        requireNonNull(context, "context is null");
        this.source = requireNonNull(source, "source is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.passThroughThreshold = requireNonNull(passThroughThreshold, "passThroughThreshold is null");
    }

    @Override
    public Result execute()
    {
        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Finished finished -> finished;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (memory; page) {
                    yield processPage(page);
                }
            }
        };
    }

    private Result processPage(@Borrow GpuPage page)
    {
        if (dynamicFilter.isPresent()) {
            return dynamicFilter.get().useCurrentFilter(currentDynamicFilter -> applyFilters(currentDynamicFilter, page));
        }
        return applyFilters(new CompiledDynamicFilter.All(), page);
    }

    private Result applyFilters(CompiledDynamicFilter currentDynamicFilter, @Borrow GpuPage page)
    {
        return switch (currentDynamicFilter) {
            case CompiledDynamicFilter.All() -> {
                if (staticFilter.isEmpty()) {
                    yield new Data(AllocatedMemory.untracked(), page.shallowCopy());
                }
                try (ColumnVector mask = computeMask(page, staticFilter.get())) {
                    yield applyMask(page, mask, OptionalDouble.empty())
                            .<Result>map(maskedPage -> new Data(AllocatedMemory.untracked(), maskedPage))
                            .orElseGet(Yielded::new);
                }
            }
            case CompiledDynamicFilter.None() -> new Finished();
            case CompiledDynamicFilter.Expression(CompiledExpression expression) -> {
                try (ClosingOnce<ColumnVector> dynamicFilterMask = ClosingOnce.own(computeMask(page, expression))) {
                    if (staticFilter.isEmpty()) {
                        yield applyMask(page, dynamicFilterMask.borrow(), passThroughThreshold)
                                .<Result>map(maskedPage -> new Data(AllocatedMemory.untracked(), maskedPage))
                                .orElseGet(Yielded::new);
                    }
                    try (ClosingOnce<ColumnVector> staticFilterMask = ClosingOnce.own(computeMask(page, staticFilter.get()))) {
                        try (ColumnVector mask = dynamicFilterMask.borrow().binaryOp(BinaryOp.NULL_LOGICAL_AND, staticFilterMask.borrow(), DType.BOOL8)) {
                            dynamicFilterMask.close();
                            staticFilterMask.close();
                            yield applyMask(page, mask, OptionalDouble.empty())
                                    .<Result>map(maskedPage -> new Data(AllocatedMemory.untracked(), maskedPage))
                                    .orElseGet(Yielded::new);
                        }
                    }
                }
            }
        };
    }

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(source);
            dynamicFilter.ifPresent(dynamicFilter -> closer.register(dynamicFilter::operatorClosed));
        }
    }

    private static @Move ColumnVector computeMask(@Borrow GpuPage input, CompiledExpression filter)
    {
        List<Integer> inputChannels = filter.inputChannels().getInputChannels();
        List<@Borrow ColumnVector> inputs = inputChannels.stream()
                .map(input::column)
                .map(DeviceMemory.class::cast)
                .map(DeviceMemory::columnVector)
                .collect(toImmutableList());

        return filter.expression().evaluate(input.positionCount(), inputs);
    }

    private static @Move Optional<@Own GpuPage> applyMask(@Borrow GpuPage input, @Borrow ColumnVector mask, OptionalDouble passThroughThresholdRatio)
    {
        try (Scalar sum = mask.sum(DType.INT32)) {
            int retained = sum.isValid() ? sum.getInt() : 0;
            if (retained == 0) {
                return Optional.empty();
            }
            if (retained == input.positionCount() || (passThroughThresholdRatio.isPresent() && retained >= input.positionCount() * passThroughThresholdRatio.getAsDouble())) {
                return Optional.of(input.shallowCopy());
            }

            try (Table table = toTable(input);
                    Table filtered = table.filter(mask)) {
                verify(filtered.getRowCount() == retained, "Row count after filter does not match mask's retained: %s != %s", filtered.getRowCount(), retained);
                return Optional.of(toGpuPage(filtered));
            }
        }
    }
}
