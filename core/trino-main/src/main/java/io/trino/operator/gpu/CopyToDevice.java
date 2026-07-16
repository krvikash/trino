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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.GpuTypeConversion.ToColumn;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CopyToDevice
        implements GpuOperation
{
    private final GpuTaskMemoryContext taskMemoryContext;
    private final GpuOperation source;
    private final List<Type> types;
    private final int columnCount;
    private final Set<Integer> copyColumns;

    public CopyToDevice(Context context, GpuOperation source, List<Type> types, Set<Integer> copyColumns)
    {
        this.taskMemoryContext = context.taskMemoryContext();
        this.source = requireNonNull(source, "source is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columnCount = types.size();
        this.copyColumns = ImmutableSet.copyOf(requireNonNull(copyColumns, "copyColumns is null"));
        checkArgument(!copyColumns.isEmpty(), "No columns to copy");
        copyColumns.forEach(column -> checkArgument(
                0 <= column && column < columnCount,
                "Invalid column to copy: %s, there are %s columns",
                column,
                columnCount));
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
                    yield processPage(memory, page);
                }
            }
        };
    }

    private @Move Data processPage(@Borrow AllocatedMemory pageAllocation, @Borrow GpuPage page)
    {
        checkArgument(page.columnCount() == columnCount, "Page has wrong column count");
        @Own Column[] newColumns = new Column[page.columnCount()];
        try (ClosingRef<AllocatedMemory> allocation = ClosingRef.own(taskMemoryContext.allocate(getClass().getSimpleName(), MemoryAmount.ZERO))) {
            allocation.borrow().transferFrom(pageAllocation); // page will be closed
            for (int columnIndex = 0; columnIndex < page.columnCount(); columnIndex++) {
                if (copyColumns.contains(columnIndex)) {
                    newColumns[columnIndex] = switch (page.column(columnIndex)) {
                        case Blocks blocks -> {
                            Type type = types.get(columnIndex);
                            ToColumn toColumn = GpuTypeConversion.toGpuMapping(type)
                                    .orElseThrow(() -> new UnsupportedOperationException("Unsupported type: " + type))
                                    .toColumn();
                            yield new DeviceMemory(toColumn.copyToDevice(blocks));
                        }
                        // already on the device
                        case DeviceMemory deviceMemory -> deviceMemory.incRefCount();
                    };
                }
                else {
                    newColumns[columnIndex] = page.column(columnIndex).incRefCount();
                }
            }

            try (ClosingRef<GpuPage> gpuPage = ClosingRef.own(new GpuPage(page.positionCount(), newColumns))) {
                allocation.borrow().update(gpuPage.borrow().retainedMemory());
                return new Data(allocation.take(), gpuPage.take());
            }
        }
        finally {
            for (Column column : newColumns) {
                if (column != null) {
                    column.close();
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
