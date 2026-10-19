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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.ReductionAggregation;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import static com.google.common.base.Verify.verify;

public final class GpuJoinUtils
{
    private GpuJoinUtils() {}

    /// Device memory a join output chunk is aimed at when the output does not fit a single chunk.
    public static final long TARGET_OUTPUT_CHUNK_BYTES = 128L * 1024 * 1024;
    /// Device memory a join output chunk may not exceed. Higher than [#TARGET_OUTPUT_CHUNK_BYTES]
    /// because a chunk sized from this bound is not known to fit exactly, unlike one sized from a
    /// page's average row.
    public static final long MAX_OUTPUT_CHUNK_BYTES = 1024L * 1024 * 1024;
    /// Rows an output chunk may not exceed, on top of the byte limits. A cuDF column cannot hold more
    /// than [Integer#MAX_VALUE] rows, so a chunk of cheap rows still has to be bounded by their count.
    public static final int MAX_OUTPUT_CHUNK_ROWS = 10_000_000;

    /// Largest device footprint a single row of `table` contributes to a gathered output row. Rows of a
    /// variable-width column differ in width, so the widest is reduced out of them on the device.
    public static long maxBytesPerRow(GpuTaskMemoryContext taskMemoryContext, String allocationTag, @Borrow Table table, int rowCount)
    {
        long maxBytesPerRow = 0;
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            ColumnVector column = table.getColumn(i);
            if (!column.getType().hasOffsets()) {
                // One byte covers the column's share of the validity mask
                maxBytesPerRow += column.getType().getSizeInBytes() + 1;
                continue;
            }
            maxBytesPerRow += Integer.BYTES + 1 + maxVariableWidthBytes(taskMemoryContext, allocationTag, column, rowCount);
        }
        return maxBytesPerRow;
    }

    /// The byte counts are on the device only while the maximum is reduced out of them, so the
    /// reservation covers just that and only the single reduced value is kept.
    private static long maxVariableWidthBytes(GpuTaskMemoryContext taskMemoryContext, String allocationTag, @Borrow ColumnVector column, int rowCount)
    {
        // The byte counts of one column, plus the validity mask copied along with them
        MemoryAmount byteCountsBytes = MemoryAmount.gpuDevice((long) rowCount * (Integer.BYTES + 1));
        try (AllocatedMemory _ = taskMemoryContext.allocate(allocationTag, byteCountsBytes);
                ColumnVector byteCounts = variableWidthByteCounts(column);
                Scalar maxByteCount = byteCounts.reduce(ReductionAggregation.max(), DType.INT32)) {
            // The reduction is invalid only when every row is null, and a null holds no bytes of its own
            return maxByteCount.isValid() ? maxByteCount.getInt() : 0;
        }
    }

    /// Bytes each row of a variable-width column holds. VARBINARY is the only [DType#LIST] the GPU
    /// produces and it is a list of single bytes, so its element count is its byte count.
    private static @Move ColumnVector variableWidthByteCounts(@Borrow ColumnVector column)
    {
        if (column.getType().equals(DType.STRING)) {
            return column.getByteCount();
        }
        verify(column.getType().equals(DType.LIST), "Unexpected variable-width type: %s", column.getType());
        try (ColumnView elements = column.getChildColumnView(0)) {
            verify(elements.getType().equals(DType.UINT8), "Unexpected list element type: %s", elements.getType());
        }
        return column.countElements();
    }
}
