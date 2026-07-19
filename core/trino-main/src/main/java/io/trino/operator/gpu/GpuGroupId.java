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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.operator.GroupIdOperator;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getNullColumnMemoryUsage;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static java.util.Objects.requireNonNull;

/**
 * GPU operation equivalent of {@link GroupIdOperator}.
 */
public class GpuGroupId
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final List<Map<Integer, Integer>> groupingSetMappings;
        private final List<DType> outputTypes;

        public Factory(List<Map<Integer, Integer>> groupingSetMappings, List<DType> outputTypes)
        {
            this.groupingSetMappings = groupingSetMappings.stream()
                    .map(ImmutableMap::copyOf)
                    .collect(toImmutableList());
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(groupingSetMappings, outputTypes);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            int[][] groupingSetInputs = new int[groupingSetMappings.size()][outputTypes.size() - 1];
            for (int s = 0; s < groupingSetInputs.length; s++) {
                Arrays.fill(groupingSetInputs[s], -1);
                for (Map.Entry<Integer, Integer> entry : groupingSetMappings.get(s).entrySet()) {
                    groupingSetInputs[s][entry.getKey()] = entry.getValue();
                }
            }
            return new GpuGroupId(context, source, groupingSetInputs, outputTypes);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuTaskMemoryContext taskMemoryContext;
    private final GpuOperation source;
    // [setId][outputChannel] = inputChannel, or -1 meaning NULL
    private final int[][] groupingSetInputs;
    private final List<DType> outputTypes;

    private final ClosingRef<AllocatedMemory> allocated;
    private @Own GpuPage currentPage;
    private int currentGroupingSet;
    private boolean finished;

    public GpuGroupId(Context context, GpuOperation source, int[][] groupingSetInputs, List<DType> outputTypes)
    {
        this.taskMemoryContext = context.taskMemoryContext();
        this.source = requireNonNull(source, "source is null");
        this.groupingSetInputs = requireNonNull(groupingSetInputs, "groupingSetInputs is null");
        this.outputTypes = requireNonNull(outputTypes, "outputTypes is null");
        this.allocated = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
    }

    @Override
    public Result execute()
    {
        if (finished) {
            return new Finished();
        }

        if (currentPage != null) {
            return generateNextPage();
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Finished() -> {
                finished = true;
                yield new Finished();
            }
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (memory) {
                    currentPage = page;
                    allocated.borrow().transferFrom(memory);
                }
                yield generateNextPage();
            }
        };
    }

    private @Move Data generateNextPage()
    {
        int setId = currentGroupingSet;
        int positionCount = currentPage.positionCount();
        MemoryAmount estimatedNonSharedColumnsMemory = estimateNonSharedColumnsMemory(setId, positionCount);
        int[] inputs = groupingSetInputs[setId];
        @Own Column[] outputColumns = new Column[inputs.length + 1];
        try (ClosingRef<AllocatedMemory> outputAllocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), estimatedNonSharedColumnsMemory))) {
            long nonSharedColumnsMemoryBytes = 0L;

            for (int i = 0; i < inputs.length; i++) {
                if (inputs[i] == -1) {
                    try (Scalar nullScalar = Scalar.fromNull(outputTypes.get(i))) {
                        DeviceMemory nullColumn = new DeviceMemory(ColumnVector.fromScalar(nullScalar, positionCount));
                        outputColumns[i] = nullColumn;
                        nonSharedColumnsMemoryBytes += nullColumn.retainedDeviceMemoryBytes();
                    }
                }
                else {
                    outputColumns[i] = currentPage.column(inputs[i]).incRefCount();
                }
            }

            try (Scalar groupIdScalar = Scalar.fromLong(setId)) {
                DeviceMemory groupIdColumn = new DeviceMemory(ColumnVector.fromScalar(groupIdScalar, positionCount));
                outputColumns[outputColumns.length - 1] = groupIdColumn;
                nonSharedColumnsMemoryBytes += groupIdColumn.retainedDeviceMemoryBytes();
            }

            currentGroupingSet = (currentGroupingSet + 1) % groupingSetInputs.length;
            try (ClosingRef<GpuPage> outputPage = ClosingRef.own(new GpuPage(positionCount, outputColumns))) {
                if (currentGroupingSet == 0) {
                    outputAllocation.borrow().transferFrom(allocated.borrow());
                    currentPage.close();
                    currentPage = null;
                    outputAllocation.borrow().update(outputPage.borrow().retainedMemory());
                }
                else {
                    outputAllocation.borrow().update(MemoryAmount.gpuDevice(nonSharedColumnsMemoryBytes));
                }
                return new Data(outputAllocation.take(), outputPage.take());
            }
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private MemoryAmount estimateNonSharedColumnsMemory(int setId, int positionCount)
    {
        int[] inputs = groupingSetInputs[setId];
        long bytes = 0;
        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] == -1) {
                bytes += getNullColumnMemoryUsage(outputTypes.get(i), positionCount);
            }
        }
        // groupId column
        bytes += (long) DType.INT64.getSizeInBytes() * positionCount;
        return MemoryAmount.gpuDevice(bytes);
    }

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(allocated);
            if (currentPage != null) {
                closer.register(currentPage);
                currentPage = null;
            }
            closer.register(source);
        }
    }
}
