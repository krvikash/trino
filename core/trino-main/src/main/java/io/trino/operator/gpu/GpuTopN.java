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
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.GpuSortUtils.estimateOrderByMemoryUsage;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.base.gpu.GpuUtils.toGpuPage;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Accumulates input pages into a single cuDF Table, sorting and truncating to {@code limit}
 * rows after each page so the buffered state never exceeds the final result size. Once the
 * source is finished the truncated table is emitted as a single GpuPage.
 */
public final class GpuTopN
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final int limit;
        private final int[] sortChannels;
        private final List<SortOrder> sortOrders;
        private final List<Type> sortTypes;

        public Factory(int limit, int[] sortChannels, List<SortOrder> sortOrders, List<Type> sortTypes)
        {
            checkArgument(sortChannels.length == sortOrders.size(), "sortChannels and sortOrders sizes do not match");
            checkArgument(sortChannels.length == sortTypes.size(), "sortChannels and sortTypes sizes do not match");
            this.limit = limit;
            this.sortChannels = sortChannels.clone();
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
            this.sortTypes = ImmutableList.copyOf(requireNonNull(sortTypes, "sortTypes is null"));
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(limit, sortChannels, sortOrders, sortTypes);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuTopN(context, source, limit, sortChannels, sortOrders, sortTypes);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuOperation source;
    private final int limit;
    private final int[] sortChannels;
    private final List<SortOrder> sortOrders;
    private final List<Type> sortTypes;

    // Partial Top N data. Temporarily unsorted when handing new input.
    private final ClosingRef<Table> buffered = ClosingRef.empty();
    private final ClosingRef<AllocatedMemory> allocated;
    private boolean finished;

    private GpuTopN(Context context, GpuOperation source, int limit, int[] sortChannels, List<SortOrder> sortOrders, List<Type> sortTypes)
    {
        this.source = requireNonNull(source, "source is null");
        this.limit = limit;
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.sortTypes = ImmutableList.copyOf(requireNonNull(sortTypes, "sortTypes is null"));
        allocated = ClosingRef.own(context.taskMemoryContext().allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
    }

    @Override
    public @Move Result execute()
    {
        if (finished) {
            return new Finished();
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory allocation, GpuPage page) -> {
                // Absorb reservation -- we will take or close the page
                try (allocation) {
                    allocated.borrow().transferFrom(allocation);
                }
                long combinedBytes = allocated.borrow().amount().gpuDeviceBytes();
                long sortKeyBytes = getSortKeyBytes(page);
                long rowCount = (buffered.isEmpty() ? 0 : buffered.borrow().getRowCount()) + page.positionCount();
                long workBytes = estimateOrderByMemoryUsage(getSortDTypes(page), sortTypes, sortOrders, sortKeyBytes, combinedBytes, rowCount);
                allocated.borrow().update(MemoryAmount.gpuDevice(workBytes));
                try (page) {
                    accumulate(page);
                }
                sortAndTruncate();
                allocated.borrow().update(MemoryAmount.gpuDevice(buffered.borrow().getDeviceMemorySize()));
                yield new Yielded();
            }
            case Finished() -> {
                finished = true;
                if (!buffered.isEmpty()) {
                    try (Table table = buffered.take()) {
                        yield new Data(allocated.take(), toGpuPage(table));
                    }
                }
                yield new Finished();
            }
        };
    }

    private DType[] getSortDTypes(@Borrow GpuPage page)
    {
        return Arrays.stream(sortChannels)
                .mapToObj(page::column)
                .map(column -> ((Column.DeviceMemory) column).columnVector().getType())
                .toArray(DType[]::new);
    }

    private long getSortKeyBytes(@Borrow GpuPage page)
    {
        long bytes = 0;
        for (int channel : sortChannels) {
            if (!buffered.isEmpty()) {
                bytes += buffered.borrow().getColumn(channel).getDeviceMemorySize();
            }
            bytes += page.column(channel).retainedDeviceMemoryBytes();
        }
        return bytes;
    }

    private void accumulate(@Borrow GpuPage page)
    {
        try (ClosingRef<Table> incoming = ClosingRef.own(toTable(page))) {
            if (buffered.isEmpty()) {
                buffered.set(incoming.take());
                return;
            }

            try (Table accumulated = buffered.take()) {
                buffered.set(Table.concatenate(accumulated, incoming.borrow()));
            }
        }
    }

    private void sortAndTruncate()
    {
        try (Table unsorted = buffered.take();
                Table sorted = GpuSortUtils.orderBy(unsorted, sortChannels, sortOrders, sortTypes)) {
            buffered.set(applyLimit(sorted, limit));
        }
    }

    private static @Move Table applyLimit(@Borrow Table sorted, int limit)
    {
        int retainedRows = min(toIntExact(sorted.getRowCount()), limit);
        @Own ColumnVector[] columns = new ColumnVector[sorted.getNumberOfColumns()];
        try {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = sorted.getColumn(i).subVector(0, retainedRows);
            }
            return new Table(columns);
        }
        finally {
            closeColumns(columns);
        }
    }

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(source);
            closer.register(allocated); // release after buffered is closed
            closer.register(buffered);
        }
    }
}
