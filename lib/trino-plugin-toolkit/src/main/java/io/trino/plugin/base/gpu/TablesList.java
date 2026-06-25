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
package io.trino.plugin.base.gpu;

import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.annotation.NotThreadSafe;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;

@NotThreadSafe
public final class TablesList
        implements RuntimeCloseable
{
    public static TablesList create()
    {
        return new TablesList();
    }

    private final List<@Own Table> tables = new ArrayList<>();
    private boolean closed;

    private TablesList() {}

    public void add(@Move Table table)
    {
        if (closed) {
            IllegalStateException error = new IllegalStateException("Already closed");
            closeAllSuppress(error, table);
            throw error;
        }
        tables.add(table);
    }

    public @Borrow List<Table> borrow()
    {
        checkState(!closed, "Already closed");
        return ImmutableList.copyOf(tables);
    }

    public boolean isEmpty()
    {
        checkState(!closed, "Already closed");
        return tables.isEmpty();
    }

    public MemoryAmount concatenateMemoryRequirements()
    {
        checkState(!closed, "Already closed");
        return switch (tables.size()) {
            case 0, 1 -> MemoryAmount.ZERO;
            default -> MemoryAmount.gpuDevice(tables.stream().mapToLong(Table::getDeviceMemorySize).sum());
        };
    }

    public @Move Table concatenateAndClear()
    {
        checkState(!closed, "Already closed");
        switch (tables.size()) {
            case 0 -> throw new NoSuchElementException("Empty");
            case 1 -> {
                Table only = getOnlyElement(tables);
                tables.clear();
                return only;
            }
            default -> {
                try (var closer = UncheckedCloser.create()) {
                    Table[] toConcatenate = tables.stream()
                            .peek(table -> closer.register(table::close))
                            .toArray(Table[]::new);
                    tables.clear();
                    return Table.concatenate(toConcatenate);
                }
            }
        }
    }

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            tables.forEach(table -> closer.register(table::close));
            tables.clear();
            closed = true;
        }
    }
}
