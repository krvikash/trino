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
import ai.rapids.cudf.OrderByArg;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.GpuUtils.closeColumns;
import static io.trino.operator.gpu.GpuUtils.concatenateAndClose;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuTopN
        implements GpuOperation
{
    public static class Factory
            implements GpuOperation.Factory
    {
        private final int limit;
        private final int[] sortChannels;
        private final List<SortOrder> sortOrders;

        public Factory(int limit, int[] sortChannels, List<SortOrder> sortOrders)
        {
            checkArgument(sortChannels.length == sortOrders.size(), "sortChannels and sortOrders sizes do not match");
            this.limit = limit;
            this.sortChannels = sortChannels.clone();
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuTopN(source, limit, sortChannels, sortOrders);
        }
    }

    private final GpuOperation source;
    private final int limit;
    private final int[] sortChannels;
    private final List<SortOrder> sortOrders;

    private final List<@Own Table> inputTables = new ArrayList<>();
    private long totalBufferedRowCount;
    private @Nullable @Own GpuPage result;
    private boolean finished;

    private GpuTopN(GpuOperation source, int limit, int[] sortChannels, List<SortOrder> sortOrders)
    {
        this.source = requireNonNull(source, "source is null");
        this.limit = limit;
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
    }

    @Override
    public @Move Result execute()
    {
        // TODO: Currently, we buffer input GpuPages until source is finished, then sort and return the top N rows.
        //  This approach is suboptimal and should be improved: https://starburstdata.atlassian.net/browse/ENG-10569

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
                Optional<GpuPage> topNResult = computeTopN();
                if (topNResult.isEmpty()) {
                    finished = true;
                    yield new Finished();
                }
                result = topNResult.get();
                yield new Yielded();
            }
        };
    }

    private void bufferPage(GpuPage page)
    {
        totalBufferedRowCount += page.positionCount();

        @Borrow ColumnVector[] columns = new ColumnVector[page.columnCount()];
        for (int i = 0; i < page.columnCount(); i++) {
            columns[i] = ((DeviceMemory) page.column(i)).columnVector();
        }
        inputTables.add(new Table(columns));
    }

    private @Move Optional<GpuPage> computeTopN()
    {
        if (totalBufferedRowCount == 0) {
            return Optional.empty();
        }

        try (Table concatenated = concatenateAndClose(inputTables)) {
            try (Table sorted = concatenated.orderBy(toOrderByArgs())) {
                return Optional.of(applyLimit(sorted, limit));
            }
        }
    }

    private OrderByArg[] toOrderByArgs()
    {
        OrderByArg[] args = new OrderByArg[sortChannels.length];
        for (int i = 0; i < sortChannels.length; i++) {
            SortOrder order = sortOrders.get(i);
            args[i] = order.isAscending()
                    ? OrderByArg.asc(sortChannels[i], order.isNullsFirst())
                    : OrderByArg.desc(sortChannels[i], !order.isNullsFirst());
        }
        return args;
    }

    private static @Move GpuPage applyLimit(@Borrow Table sorted, int limit)
    {
        int rowCount = Math.min(toIntExact(sorted.getRowCount()), limit);

        @Own Column[] columns = new Column[sorted.getNumberOfColumns()];
        try {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new DeviceMemory(sorted.getColumn(i).subVector(0, rowCount));
            }
            return new GpuPage(rowCount, columns);
        }
        finally {
            closeColumns(columns);
        }
    }

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
