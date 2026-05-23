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
package io.trino.operator.gpu.exchange;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.HashType;
import ai.rapids.cudf.PartitionedTable;
import ai.rapids.cudf.Table;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.GpuUtils.closeColumns;

/**
 * Hash-partitions a {@link GpuPage} across N partitions using cuDF {@code hashPartition}.
 */
final class GpuPartitioner
{
    private GpuPartitioner() {}

    /**
     * Hash-partition {@code input} into {@code numPartitions} parts using {@code keyChannels} as
     * the hash key. Some entries in the returned array may be zero-row pages.
     */
    static @Move GpuPage[] partition(@Borrow GpuPage input, int[] keyChannels, int numPartitions)
    {
        int columnCount = input.columnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            checkArgument(input.column(columnIndex) instanceof DeviceMemory,
                    "All columns must be DeviceMemory; column %s is %s",
                    columnIndex,
                    input.column(columnIndex).getClass().getSimpleName());
        }

        @Borrow ColumnVector[] tableColumns = new ColumnVector[columnCount];
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            tableColumns[columnIndex] = ((DeviceMemory) input.column(columnIndex)).columnVector();
        }

        // Table constructor incRefCount()s each column, so the cuDF Table holds an independent ref.
        try (Table inputTable = new Table(tableColumns)) {
            return splitTable(inputTable, input.positionCount(), columnCount, keyChannels, numPartitions);
        }
    }

    // Refcount invariant: every ContiguousTable in `splits` is closed in the outer finally,
    // dropping the original refcount that contiguousSplit incremented. contiguousTableToPage
    // builds each output GpuPage via incRefCount(), so closing the split does not free the
    // underlying device buffers. If any contiguousTableToPage throws, the inner catch closes
    // GpuPages already built, which releases their refcounts symmetrically.
    private static @Move GpuPage[] splitTable(
            Table inputTable,
            int positionCount,
            int columnCount,
            int[] keyChannels,
            int numPartitions)
    {
        @Own ContiguousTable[] splits = computeSplits(inputTable, positionCount, keyChannels, numPartitions);
        try {
            GpuPage[] result = new GpuPage[numPartitions];
            try {
                for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                    result[partitionIndex] = contiguousTableToPage(splits[partitionIndex], columnCount);
                }
                return result;
            }
            catch (Throwable e) {
                for (GpuPage page : result) {
                    if (page != null) {
                        try {
                            page.close();
                        }
                        catch (Throwable suppressed) {
                            e.addSuppressed(suppressed);
                        }
                    }
                }
                throw e;
            }
        }
        finally {
            for (ContiguousTable contiguousTable : splits) {
                contiguousTable.close();
            }
        }
    }

    private static @Own ContiguousTable[] computeSplits(Table inputTable, int positionCount, int[] keyChannels, int numPartitions)
    {
        if (positionCount == 0) {
            // contiguousSplit on an empty table with N-1 split points produces N zero-row splits.
            int[] splitPoints = new int[numPartitions - 1];
            return inputTable.contiguousSplit(splitPoints);
        }
        try (PartitionedTable hashed = inputTable.onColumns(keyChannels)
                .hashPartition(HashType.MURMUR3, numPartitions)) {
            // hashed.getPartitions()[i] is the row offset where partition i starts;
            // contiguousSplit wants cut points i.e. the offsets after the first.
            int[] offsets = hashed.getPartitions();
            int[] splitPoints = new int[numPartitions - 1];
            System.arraycopy(offsets, 1, splitPoints, 0, splitPoints.length);
            return hashed.getTable().contiguousSplit(splitPoints);
        }
    }

    private static @Move GpuPage contiguousTableToPage(@Borrow ContiguousTable contiguousTable, int columnCount)
    {
        Table partitionTable = contiguousTable.getTable();
        int rowCount = (int) contiguousTable.getRowCount();
        @Own Column[] columns = new Column[columnCount];
        try {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                columns[columnIndex] = new DeviceMemory(partitionTable.getColumn(columnIndex).incRefCount());
            }
            return new GpuPage(rowCount, columns);
        }
        finally {
            closeColumns(columns);
        }
    }
}
