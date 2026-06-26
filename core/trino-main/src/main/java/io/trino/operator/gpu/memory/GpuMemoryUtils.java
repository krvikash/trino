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
import ai.rapids.cudf.Table;

public final class GpuMemoryUtils
{
    private GpuMemoryUtils() {}

    /// Estimate the additional GPU device memory retained by `new HashJoin(buildKeys, [false])`,
    /// assuming `buildKeys` is retained by the caller and therefore not counted here.
    // TODO might become obsolete when https://github.com/rapidsai/cudf/issues/22965 is done
    public static long getHashJoinAdditionalGpuDeviceMemoryUsage(Table buildKeys)
    {
        long rowCount = buildKeys.getRowCount();
        if (rowCount == 0) {
            return 0;
        }
        // cuco::static_multiset with CUCO_DESIRED_LOAD_FACTOR=0.5, storage<2>, cooperative group size=2:
        // rawCapacity = ceil(rowCount / 0.5) = 2 * rowCount
        // capacity = CG_SIZE * ceilDiv(rawCapacity, CG_SIZE * WINDOW_SIZE) * WINDOW_SIZE
        //          = 4 * ceilDiv(rowCount, 2)
        // Each slot holds cuco::pair<uint32_t, int32_t> = 8 bytes
        long cucoBytes = 4L * ((rowCount + 1) / 2) * (Integer.BYTES + Integer.BYTES);
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
        // For large allocations, cuco's storage appears in the RMM ASYNC pool as two separately
        // 2MB-rounded chunks, so the effective rounding is 4MB. Add a 1% safety margin for internal overhead.
        long twoMb = 2L * 1024 * 1024;
        long fourMb = 4L * 1024 * 1024;
        if (cucoBytes >= fourMb) {
            long rounded = ((cucoBytes + fourMb - 1) / fourMb) * fourMb;
            return rounded + rounded / 100 + preprocessedBytes;
        }
        // Medium allocations (2MB..4MB) are not rounded by the pool, but have slightly larger internal overhead
        if (cucoBytes >= twoMb) {
            return cucoBytes + 1024 + preprocessedBytes;
        }
        // For small allocations (<2MB), add empirically observed fixed cuco internal overhead
        return cucoBytes + 304 + preprocessedBytes;
    }
}
