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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.operator.OperatorContext;
import io.trino.operator.TestingOperatorContext;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class TestingGpuOperationContext
        implements GpuOperation.Context
{
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newSingleThreadScheduledExecutor(daemonThreadsNamed("testing-gpu-operation-context-%s"));

    private final ReservationHandler heapMemoryHandler = new ReservationHandler();
    private final ReservationHandler gpuDeviceMemoryHandler = new ReservationHandler();
    private final ReservationHandler offHeapMemoryHandler = new ReservationHandler();

    private final AggregatedMemoryContext heapMemory = newRootAggregatedMemoryContext(heapMemoryHandler, 0);
    private final AggregatedMemoryContext gpuDeviceMemory = newRootAggregatedMemoryContext(gpuDeviceMemoryHandler, 0);
    private final AggregatedMemoryContext offHeapMemory = newRootAggregatedMemoryContext(offHeapMemoryHandler, 0);

    private final GpuTaskMemoryContext taskMemoryContext = new GpuTaskMemoryContext(heapMemory, gpuDeviceMemory, offHeapMemory);
    private final OperatorContext operatorContext = TestingOperatorContext.create(SCHEDULED_EXECUTOR);

    @Override
    public GpuTaskMemoryContext taskMemoryContext()
    {
        return taskMemoryContext;
    }

    @Override
    public OperatorContext operatorContext()
    {
        return operatorContext;
    }

    public void setGpuDeviceMemoryReservationListener(ReservationListener listener)
    {
        gpuDeviceMemoryHandler.setListener(listener);
    }

    public void removeGpuDeviceMemoryReservationListener(ReservationListener listener)
    {
        gpuDeviceMemoryHandler.removeListener(listener);
    }

    private static final class ReservationHandler
            implements MemoryReservationHandler
    {
        @GuardedBy("this")
        private long reservedMemory;
        @GuardedBy("this")
        private ReservationListener listener;

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            requireNonNull(allocationTag, "allocationTag is null");
            // allocations tags are not tracked

            synchronized (this) {
                reservedMemory = addExact(reservedMemory, delta);
                if (listener != null) {
                    // Call listener under a lock, otherwise notifications could be delivered out of order.
                    // (This likely does not actually matter, as the calling memory context likely synchronizes anyway.)
                    listener.onReservationChange(reservedMemory, delta);
                }
            }
            return NOT_BLOCKED;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void transferTags(String fromTag, String toTag, long bytes)
        {
            requireNonNull(fromTag, "fromTag is null");
            requireNonNull(toTag, "toTag is null");
            // allocations tags are not tracked
        }

        private synchronized void setListener(ReservationListener listener)
        {
            checkState(this.listener == null, "listener already set");
            this.listener = requireNonNull(listener, "listener is null");
        }

        private synchronized void removeListener(ReservationListener listener)
        {
            requireNonNull(listener, "listener is null");
            checkState(this.listener == listener, "listener not set");
            this.listener = null;
        }
    }

    @ThreadSafe
    public interface ReservationListener
    {
        void onReservationChange(long currentReservation, long delta);
    }
}
