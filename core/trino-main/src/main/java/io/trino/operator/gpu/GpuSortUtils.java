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
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.OrderByArg;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getOrderByMemoryUsage;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static java.util.Objects.requireNonNull;

public final class GpuSortUtils
{
    private GpuSortUtils() {}

    /// Sorts the table by the given sort channels and orders following Trino semantics.
    public static @Move Table orderBy(@Borrow Table table, int[] sortChannels, List<SortOrder> sortOrders, List<Type> sortTypes)
    {
        checkArgument(sortChannels.length == sortOrders.size(), "sortChannels and sortOrders sizes do not match");
        checkArgument(sortChannels.length == sortTypes.size(), "sortChannels and sortTypes sizes do not match");

        // cuDF always sorts NaN after real values, while Trino uses COMPARISON_UNORDERED_FIRST
        // for NULLS_FIRST orders, placing NaN before real values (after null). NULLS_LAST already matches cuDF.
        //
        // When correction is needed, insert a temporary boolean flag column before the float sort key.
        // For ASC: isNotNan() → false(NaN) before true(non-NaN) — NaN at the front.
        // For DESC: isNan() → false(non-NaN) before true(NaN) — NaN at the back.
        // Use sortOrder() on the expanded keys to get row indices, then gather() the original table
        // so the flag columns are never materialized in the sorted output.

        int flagCount = countNanFlags(sortTypes, sortOrders);
        if (flagCount == 0) {
            return table.orderBy(toOrderByArgs(sortChannels, sortOrders));
        }
        try (SortableTable sortable = appendNanFlags(table, sortChannels, sortTypes, sortOrders, flagCount)) {
            OrderByArg[] args = toOrderByArgs(sortable.sortChannels(), sortable.sortOrders());
            try (ColumnVector sortIndices = sortable.table().sortOrder(args)) {
                return table.gather(sortIndices);
            }
        }
    }

    public static long estimateOrderByMemoryUsage(DType[] sortDTypes, List<Type> sortTypes, List<SortOrder> sortOrders, long sortKeyBytes, long totalBytes, long rowCount)
    {
        checkArgument(sortDTypes.length == sortOrders.size(), "sortDTypes and sortOrders sizes do not match");
        checkArgument(sortDTypes.length == sortTypes.size(), "sortDTypes and sortTypes sizes do not match");

        int flagCount = countNanFlags(sortTypes, sortOrders);
        if (flagCount == 0) {
            return getOrderByMemoryUsage(sortDTypes, sortKeyBytes, totalBytes);
        }
        long flagBytes = flagCount * rowCount;
        DType[] expandedTypes = new DType[sortDTypes.length + flagCount];
        int index = 0;
        for (int i = 0; i < sortDTypes.length; i++) {
            if (needsNanFlag(sortTypes.get(i), sortOrders.get(i))) {
                expandedTypes[index++] = DType.BOOL8;
            }
            expandedTypes[index++] = sortDTypes[i];
        }
        return getOrderByMemoryUsage(expandedTypes, sortKeyBytes + flagBytes, totalBytes + flagBytes);
    }

    private static int countNanFlags(List<Type> sortTypes, List<SortOrder> sortOrders)
    {
        int count = 0;
        for (int i = 0; i < sortTypes.size(); i++) {
            if (needsNanFlag(sortTypes.get(i), sortOrders.get(i))) {
                count++;
            }
        }
        return count;
    }

    private static SortableTable appendNanFlags(Table table, int[] sortChannels, List<Type> sortTypes, List<SortOrder> sortOrders, int flagCount)
    {
        int originalColumnCount = table.getNumberOfColumns();
        @Own ColumnVector[] allColumns = new ColumnVector[originalColumnCount + flagCount];
        try {
            for (int i = 0; i < originalColumnCount; i++) {
                allColumns[i] = table.getColumn(i).incRefCount();
            }

            int[] adjustedChannels = new int[sortChannels.length + flagCount];
            List<SortOrder> adjustedOrders = new ArrayList<>();
            int appendIndex = originalColumnCount;
            int outputIndex = 0;
            for (int i = 0; i < sortChannels.length; i++) {
                int channel = sortChannels[i];
                if (needsNanFlag(sortTypes.get(i), sortOrders.get(i))) {
                    allColumns[appendIndex] = createNanFlag(table.getColumn(channel), sortOrders.get(i).isAscending());
                    adjustedChannels[outputIndex++] = appendIndex;
                    adjustedOrders.add(SortOrder.ASC_NULLS_FIRST);
                    appendIndex++;
                }
                adjustedChannels[outputIndex++] = channel;
                adjustedOrders.add(sortOrders.get(i));
            }

            Table expanded = new Table(allColumns);
            return new SortableTable(expanded, adjustedChannels, ImmutableList.copyOf(adjustedOrders));
        }
        finally {
            closeColumns(allColumns);
        }
    }

    private static boolean needsNanFlag(Type type, SortOrder sortOrder)
    {
        return (type instanceof DoubleType || type instanceof RealType) && sortOrder.isNullsFirst();
    }

    private static @Move ColumnVector createNanFlag(@Borrow ColumnView column, boolean isAscending)
    {
        // ASC: isNotNan() → false(NaN) < true(non-NaN) → NaN before reals
        // DESC: isNan() → false(non-NaN) < true(NaN) → NaN after reals
        try (ClosingRef<ColumnVector> flag = ClosingRef.own(isAscending ? column.isNotNan() : column.isNan())) {
            if (column.getNullCount() == 0) {
                return flag.take();
            }
            // isNan()/isNotNan() return false/true (not null) for null inputs, so null propagation is explicit
            try (ColumnVector isNull = column.isNull();
                    Scalar nullBool = Scalar.fromNull(DType.BOOL8)) {
                return isNull.ifElse(nullBool, flag.borrow());
            }
        }
    }

    private record SortableTable(@Own Table table, int[] sortChannels, List<SortOrder> sortOrders)
            implements AutoCloseable
    {
        public SortableTable
        {
            requireNonNull(table, "table is null");
            requireNonNull(sortChannels, "sortChannels is null");
            requireNonNull(sortOrders, "sortOrders is null");
        }

        @Override
        public void close()
        {
            table.close();
        }
    }

    private static OrderByArg[] toOrderByArgs(int[] sortChannels, List<SortOrder> sortOrders)
    {
        checkArgument(sortChannels.length == sortOrders.size(), "sortChannels and sortOrders sizes do not match");
        OrderByArg[] args = new OrderByArg[sortChannels.length];
        for (int i = 0; i < sortChannels.length; i++) {
            SortOrder order = sortOrders.get(i);
            args[i] = order.isAscending()
                    ? OrderByArg.asc(sortChannels[i], order.isNullsFirst())
                    : OrderByArg.desc(sortChannels[i], !order.isNullsFirst());
        }
        return args;
    }
}
