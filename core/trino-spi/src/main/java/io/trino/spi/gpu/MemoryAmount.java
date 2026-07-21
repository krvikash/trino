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
package io.trino.spi.gpu;

import static io.trino.spi.gpu.Preconditions.checkNonNegative;
import static java.lang.Math.addExact;
import static java.lang.Math.subtractExact;

public record MemoryAmount(long heapBytes, long gpuDeviceBytes, long offHeapBytes)
{
    public static final MemoryAmount ZERO = new MemoryAmount(0, 0, 0);

    public static MemoryAmount heap(long heapBytes)
    {
        return new MemoryAmount(heapBytes, 0, 0);
    }

    public static MemoryAmount gpuDevice(long gpuDeviceBytes)
    {
        return new MemoryAmount(0, gpuDeviceBytes, 0);
    }

    public static MemoryAmount offHeap(long offHeapBytes)
    {
        return new MemoryAmount(0, 0, offHeapBytes);
    }

    public MemoryAmount
    {
        checkNonNegative(heapBytes, "heapBytes");
        checkNonNegative(gpuDeviceBytes, "gpuDeviceBytes");
        checkNonNegative(offHeapBytes, "offHeapBytes");
    }

    public MemoryAmount add(MemoryAmount other)
    {
        return new MemoryAmount(
                addExact(this.heapBytes(), other.heapBytes()),
                addExact(this.gpuDeviceBytes(), other.gpuDeviceBytes()),
                addExact(this.offHeapBytes(), other.offHeapBytes()));
    }

    public MemoryAmount subtract(MemoryAmount other)
    {
        return new MemoryAmount(
                subtractExact(this.heapBytes(), other.heapBytes()),
                subtractExact(this.gpuDeviceBytes(), other.gpuDeviceBytes()),
                subtractExact(this.offHeapBytes(), other.offHeapBytes()));
    }
}
