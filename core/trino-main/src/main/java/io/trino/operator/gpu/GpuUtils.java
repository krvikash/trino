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

import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;

public final class GpuUtils
{
    private GpuUtils() {}

    /**
     * Closes all non-null columns in the array. Elements may be null if the array was partially populated.
     */
    public static void closeColumns(@Move Column[] columns)
    {
        try (var closer = UncheckedCloser.create()) {
            for (Column column : columns) {
                if (column != null) {
                    closer.register(column);
                }
            }
        }
    }

    public static long retainedDeviceBytes(GpuPage page)
    {
        long total = 0;
        for (Column column : page.columns()) {
            if (column instanceof Column.DeviceMemory deviceMemory) {
                total += deviceMemory.columnVector().getDeviceMemorySize();
            }
        }
        return total;
    }
}
