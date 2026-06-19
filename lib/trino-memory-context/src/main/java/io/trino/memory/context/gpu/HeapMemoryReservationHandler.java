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
package io.trino.memory.context.gpu;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.MemoryAllocation;
import io.trino.spi.gpu.MemoryAmount;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public class HeapMemoryReservationHandler
        implements MemoryReservationHandler
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    @GuardedBy("this")
    private long allocated;
    @GuardedBy("this")
    private final MemoryAllocation allocation;

    public HeapMemoryReservationHandler(ConnectorGpuMemoryContext memoryContext)
    {
        this.allocated = 0;
        this.allocation = memoryContext.allocate(MemoryAmount.ZERO);
    }

    @Override
    public synchronized ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
    {
        long newAmount = allocated + delta;
        allocation.update(MemoryAmount.heap(newAmount));
        allocated = newAmount;

        return NOT_BLOCKED;
    }

    @Override
    public boolean tryReserveMemory(String allocationTag, long delta)
    {
        throw new UnsupportedOperationException();
    }
}
