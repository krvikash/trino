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
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getFilterGpuDeviceMemoryUsage;
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

    private final GpuTaskMemoryContext taskMemoryContext;
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
        this.taskMemoryContext = context.taskMemoryContext();
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
            case Data(AllocatedMemory memory, GpuPage page) -> processPage(memory, page);
        };
    }

    private Result processPage(@Move AllocatedMemory memory, @Move GpuPage page)
    {
        try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
                ClosingRef<GpuPage> inputPage = ClosingRef.own(page);
                memory) {
            allocation.borrow().transferFrom(memory);

            if (dynamicFilter.isPresent()) {
                return dynamicFilter.get().useCurrentFilter(currentDynamicFilter -> applyFilters(allocation, inputPage, currentDynamicFilter));
            }
            return applyFilters(allocation, inputPage, new CompiledDynamicFilter.All());
        }
    }

    private Result applyFilters(ClosingRef<AllocatedMemory> allocation, ClosingRef<GpuPage> inputPage, CompiledDynamicFilter currentDynamicFilter)
    {
        @Borrow GpuPage page = inputPage.borrow();
        return switch (currentDynamicFilter) {
            case CompiledDynamicFilter.All() -> {
                if (staticFilter.isEmpty()) {
                    yield toData(allocation, inputPage);
                }
                allocation.borrow().update(allocation.borrow().amount().add(getMaskMemoryAmount(page.positionCount())));
                try (ClosingOnce<ColumnVector> mask = ClosingOnce.own(computeMask(page, staticFilter.get()))) {
                    yield applyMask(allocation, inputPage, mask, OptionalDouble.empty());
                }
            }
            case CompiledDynamicFilter.None() -> new Finished();
            case CompiledDynamicFilter.Expression(CompiledExpression expression) -> {
                allocation.borrow().update(allocation.borrow().amount().add(getMaskMemoryAmount(page.positionCount())));
                try (ClosingOnce<ColumnVector> dynamicFilterMask = ClosingOnce.own(computeMask(page, expression))) {
                    if (staticFilter.isEmpty()) {
                        yield applyMask(allocation, inputPage, dynamicFilterMask, passThroughThreshold);
                    }
                    try (ClosingOnce<ColumnVector> staticFilterMask = ClosingOnce.own(computeMask(page, staticFilter.get()))) {
                        try (ClosingOnce<ColumnVector> mask = ClosingOnce.own(dynamicFilterMask.borrow().binaryOp(BinaryOp.NULL_LOGICAL_AND, staticFilterMask.borrow(), DType.BOOL8))) {
                            dynamicFilterMask.close();
                            staticFilterMask.close();
                            yield applyMask(allocation, inputPage, mask, OptionalDouble.empty());
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

        // TODO: Add memory tracking to expression evaluation: https://starburstdata.atlassian.net/browse/ENG-20209
        return filter.expression().evaluate(input.positionCount(), inputs);
    }

    private static Result applyMask(ClosingRef<AllocatedMemory> allocation, ClosingRef<GpuPage> inputPage, ClosingOnce<ColumnVector> mask, OptionalDouble passThroughThresholdRatio)
    {
        @Borrow GpuPage input = inputPage.borrow();
        try (Scalar sum = mask.borrow().sum(DType.INT32)) {
            int retained = sum.isValid() ? sum.getInt() : 0;
            if (retained == 0) {
                return new Yielded();
            }
            if (retained == input.positionCount() || (passThroughThresholdRatio.isPresent() && retained >= input.positionCount() * passThroughThresholdRatio.getAsDouble())) {
                mask.close();
                return toData(allocation, inputPage);
            }

            allocation.borrow().update(allocation.borrow().amount().add(MemoryAmount.gpuDevice(getFilterGpuDeviceMemoryUsage(input, retained))));

            @Own GpuPage outputPage;
            try (Table table = toTable(input);
                    Table filtered = table.filter(mask.borrow())) {
                verify(filtered.getRowCount() == retained, "Row count after filter does not match mask's retained: %s != %s", filtered.getRowCount(), retained);
                mask.close();
                outputPage = toGpuPage(filtered);
            }
            try (ClosingRef<GpuPage> output = ClosingRef.own(outputPage)) {
                inputPage.close();
                return toData(allocation, output);
            }
        }
    }

    private static Data toData(ClosingRef<AllocatedMemory> allocation, ClosingRef<GpuPage> inputPage)
    {
        allocation.borrow().update(inputPage.borrow().retainedMemory());
        return new Data(allocation.take(), inputPage.take());
    }

    private static MemoryAmount getMaskMemoryAmount(int positionCount)
    {
        return MemoryAmount.gpuDevice((long) positionCount * DType.BOOL8.getSizeInBytes());
    }
}
