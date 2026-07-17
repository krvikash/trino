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
package io.trino.operator.gpu.memory;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.NullEquality;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.ast.CompiledExpression;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.ceilDiv;

public final class GpuMemoryUtils
{
    private GpuMemoryUtils() {}

    // Each slot holds cuco::pair<uint32_t, int32_t> = 8 bytes
    private static final long HASH_TABLE_ENTRY_BYTES = Integer.BYTES + Integer.BYTES;

    /// Estimate the additional GPU device memory retained by `new HashJoin(buildKeys, [false])`,
    /// assuming `buildKeys` is retained by the caller and therefore not counted here.
    // TODO might become obsolete when https://github.com/rapidsai/cudf/issues/22965 is done
    public static long getHashJoinAdditionalGpuDeviceMemoryUsage(Table buildKeys)
    {
        long rowCount = buildKeys.getRowCount();
        if (rowCount == 0) {
            return 0;
        }
        long cucoBytes = cucoHashTableCapacity(rowCount) * HASH_TABLE_ENTRY_BYTES;
        // preprocessed_table::create allocates a table_device_view whose size depends on the C++
        // column_device_view children count (sizeof(column_device_view)=64 bytes each):
        // STRING has 1 C++ child (offsets), LIST has 2 C++ children (offsets + elements)
        long preprocessedBytes = 79;
        for (int i = 0; i < buildKeys.getNumberOfColumns(); i++) {
            DType.DTypeEnum typeId = buildKeys.getColumn(i).getType().getTypeId();
            if (typeId == DType.DTypeEnum.STRING) {
                preprocessedBytes += 64;
            }
            else if (typeId == DType.DTypeEnum.LIST) {
                preprocessedBytes += 128;
            }
        }
        return includePoolOverhead(cucoBytes, preprocessedBytes);
    }

    /// Estimates the peak temporary GPU device memory consumed by a single call to
    /// {@link Table#mixedInnerJoinGatherMaps(Table, Table, Table, Table, CompiledExpression, NullEquality)}.
    /// The estimate excludes the output gather maps whose size depends on join selectivity.
    public static long getMixedInnerJoinGpuDeviceMemoryUsage(long buildRows, long probeRows)
    {
        checkArgument(buildRows >= 0, "buildRows must be non-negative");
        checkArgument(probeRows >= 0, "probeRows must be non-negative");
        // cuDF swaps the probe and build sides for inner joins when buildRows > probeRows
        long hashTableRows = Math.min(buildRows, probeRows);
        long outerRows = Math.max(buildRows, probeRows);
        return estimateMixedJoinBytes(hashTableRows, outerRows, hashTableRows);
    }

    /// Estimates the peak temporary GPU device memory consumed by a single call to
    /// {@link Table#mixedLeftJoinGatherMaps(Table, Table, Table, Table, CompiledExpression, NullEquality)}.
    /// The estimate excludes the output gather maps whose size depends on join selectivity.
    public static long getMixedLeftJoinGpuDeviceMemoryUsage(long buildRows, long probeRows)
    {
        checkArgument(buildRows >= 0, "buildRows must be non-negative");
        checkArgument(probeRows >= 0, "probeRows must be non-negative");
        return estimateMixedJoinBytes(buildRows, probeRows, buildRows);
    }

    private static long estimateMixedJoinBytes(long hashTableRows, long outerRows, long bitmaskRows)
    {
        long cucoBytes = cucoHashTableCapacity(hashTableRows) * HASH_TABLE_ENTRY_BYTES;
        // precompute_mixed_join_data probes the hash table and stores per outer row:
        //   input_pairs (8B): pair<hash, row_index> — which build row matched
        //   hash_indices (8B): pair<hash, hash> — window coordinates for resuming multiset probing
        long precomputedBytes = outerRows * 16;
        // compute_mixed_join_matches_per_row: int32 count of filter-surviving matches per outer row
        long matchesPerRowBytes = outerRows * Integer.BYTES;
        // exclusive prefix sum of matchesPerRow — output write offsets per outer row
        long joinOffsetsBytes = outerRows * Integer.BYTES;
        // one bit per build row for null handling
        long bitmaskBytes = (bitmaskRows + 7) / 8;
        return includePoolOverhead(cucoBytes, precomputedBytes + matchesPerRowBytes + joinOffsetsBytes + bitmaskBytes);
    }

    private static long includePoolOverhead(long cucoBytes, long remainingBytes)
    {
        // cuco's storage<2> appears in the RMM ASYNC pool as two separately 2MB-rounded chunks,
        // so the effective rounding is 4MB for large allocations.
        // For large allocations, cuco's storage appears in the RMM ASYNC pool as two separately
        // 2MB-rounded chunks, so the effective rounding is 4MB. Add a 1% safety margin for internal overhead.
        long twoMb = 2L * 1024 * 1024;
        long fourMb = 4L * 1024 * 1024;
        if (cucoBytes >= fourMb) {
            long rounded = ((cucoBytes + fourMb - 1) / fourMb) * fourMb;
            return rounded + rounded / 100 + remainingBytes;
        }
        // Medium allocations (2MB..4MB) are not rounded by the pool, but have slightly larger internal overhead
        if (cucoBytes >= twoMb) {
            return cucoBytes + 1024 + remainingBytes;
        }
        // For small allocations (<2MB), add empirically observed fixed cuco internal overhead
        return cucoBytes + 304 + remainingBytes;
    }

    private static long cucoHashTableCapacity(long rowCount)
    {
        // cuco::static_multiset with CUCO_DESIRED_LOAD_FACTOR=0.5, storage<2>, cooperative group size=2:
        // rawCapacity = ceil(rowCount / 0.5) = 2 * rowCount
        // capacity = CG_SIZE * ceilDiv(rawCapacity, CG_SIZE * WINDOW_SIZE) * WINDOW_SIZE
        //          = 4 * ceilDiv(rowCount, 2)
        return 4L * ((rowCount + 1) / 2);
    }

    public static long getNullColumnMemoryUsage(DType type, int positionCount)
    {
        // cuDF null validity mask: one bit per row, ceil(N/8) bytes, padded to 64-byte boundary
        long nullMaskBytes = 64L * ceilDiv(ceilDiv(positionCount, 8), 64);
        int sizeInBytes = type.getSizeInBytes();
        if (sizeInBytes == 0) {
            // Variable-width: offsets array with N+1 INT32 elements (pairs of consecutive offsets
            // define start/end of each value's chars), empty chars buffer since all values are null
            return 4L * (positionCount + 1) + nullMaskBytes;
        }
        return (long) sizeInBytes * positionCount + nullMaskBytes;
    }

    public static long getFilterGpuDeviceMemoryUsage(@Borrow GpuPage input, int retainedPositionCount)
    {
        long estimatedOutputDeviceBytes = (long) (input.retainedDeviceMemoryBytes() * ((double) retainedPositionCount / input.positionCount()));
        long gatherMapBytes = (long) input.positionCount() * Integer.BYTES;
        return estimatedOutputDeviceBytes + gatherMapBytes;
    }
}
