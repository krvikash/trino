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
package io.trino.operator.torin;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.OrderByArg;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.torin.memory.GpuMemoryUtils.getOrderByMemoryUsage;
import static io.trino.plugin.base.torin.GpuUtils.closeColumns;
import static io.trino.spi.torin.GpuTypeConversion.nullScalar;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
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
        // When correction is needed, insert a temporary boolean flag column before the float sort key.
        // For ASC: isNotNan() → false(NaN) before true(non-NaN) — NaN at the front.
        // For DESC: isNan() → false(non-NaN) before true(NaN) — NaN at the back.

        int flagCount = countNanSortKeys(sortTypes, sortOrders);
        if (flagCount == 0) {
            return table.orderBy(toOrderByArgs(sortChannels, sortOrders));
        }
        try (SortableTable sortable = createSortableTable(table, sortChannels, sortTypes, sortOrders, flagCount)) {
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

        int flagCount = countNanSortKeys(sortTypes, sortOrders);
        if (flagCount == 0) {
            return getOrderByMemoryUsage(sortDTypes, sortKeyBytes, totalBytes, rowCount);
        }
        long flagBytes = multiplyExact(flagCount, rowCount);
        DType[] expandedTypes = new DType[sortDTypes.length + flagCount];
        int index = 0;
        for (int i = 0; i < sortDTypes.length; i++) {
            if (needsNanSortKey(sortTypes.get(i), sortOrders.get(i))) {
                expandedTypes[index++] = DType.BOOL8;
            }
            expandedTypes[index++] = sortDTypes[i];
        }
        return getOrderByMemoryUsage(expandedTypes, addExact(sortKeyBytes, flagBytes), addExact(totalBytes, flagBytes), rowCount);
    }

    private static int countNanSortKeys(List<Type> sortTypes, List<SortOrder> sortOrders)
    {
        int count = 0;
        for (int i = 0; i < sortTypes.size(); i++) {
            if (needsNanSortKey(sortTypes.get(i), sortOrders.get(i))) {
                count++;
            }
        }
        return count;
    }

    private static SortableTable createSortableTable(Table table, int[] sortChannels, List<Type> sortTypes, List<SortOrder> sortOrders, int flagCount)
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
                if (needsNanSortKey(sortTypes.get(i), sortOrders.get(i))) {
                    allColumns[appendIndex] = createNanSortKey(table.getColumn(channel), sortOrders.get(i).isAscending());
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

    /// Whether cuDF orders NaN values of the key differently from Trino, so the key must be preceded by a NaN sort
    /// key from [#createNanSortKey] wherever cuDF sorts or compares it in Trino order.
    public static boolean needsNanSortKey(Type type, SortOrder sortOrder)
    {
        return typeHasNaN(type) && sortOrder.isNullsFirst();
    }

    /// Creates a BOOL8 column that, ordered with [SortOrder#ASC_NULLS_FIRST] right before the key, makes cuDF
    /// order NaN values of the key the way Trino does.
    public static @Move ColumnVector createNanSortKey(@Borrow ColumnView column, boolean isAscending)
    {
        // ASC: isNotNan() → false(NaN) < true(non-NaN) → NaN before reals
        // DESC: isNan() → false(non-NaN) < true(NaN) → NaN after reals
        try (ClosingRef<ColumnVector> flag = ClosingRef.own(isAscending ? column.isNotNan() : column.isNan())) {
            if (column.getNullCount() == 0) {
                return flag.take();
            }
            // isNan()/isNotNan() return false/true (not null) for null inputs, so null propagation is explicit
            try (ColumnVector isNull = column.isNull();
                    Scalar nullBool = nullScalar(BOOLEAN)) {
                return isNull.ifElse(nullBool, flag.borrow());
            }
        }
    }

    /// Whether cuDF must treat nulls as smaller than every value to order them as the sort order does.
    public static boolean isNullSmallest(SortOrder sortOrder)
    {
        return sortOrder.isAscending() == sortOrder.isNullsFirst();
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
                    ? OrderByArg.asc(sortChannels[i], isNullSmallest(order))
                    : OrderByArg.desc(sortChannels[i], isNullSmallest(order));
        }
        return args;
    }
}
