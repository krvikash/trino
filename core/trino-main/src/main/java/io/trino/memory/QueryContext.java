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
package io.trino.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.stats.GcMonitor;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.TaskContext;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.ExceededMemoryLimitException.exceededLocalGpuMemoryLimit;
import static io.trino.ExceededMemoryLimitException.exceededLocalOffHeapMemoryLimit;
import static io.trino.ExceededMemoryLimitException.exceededLocalUserMemoryLimit;
import static io.trino.ExceededSpillLimitException.exceededPerQueryLocalLimit;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static io.trino.operator.TaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.Map.Entry.comparingByValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class QueryContext
{
    private static final Logger log = Logger.get(QueryContext.class);
    private static final long GUARANTEED_MEMORY = DataSize.of(1, MEGABYTE).toBytes();

    private final QueryId queryId;
    private final GcMonitor gcMonitor;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final long maxSpill;
    private final SpillSpaceTracker spillSpaceTracker;
    private final Map<TaskId, TaskContext> taskContexts = new ConcurrentHashMap<>();

    private volatile boolean memoryLimitsInitialized;

    // TODO: This field should be final. However, due to the way QueryContext is constructed the memory limit is not known in advance
    @GuardedBy("this")
    private long maxUserMemory;

    private final long maxGpuMemory;
    private final long maxOffHeapMemory;

    private final MemoryPool memoryPool;
    private final MemoryPool gpuDeviceMemoryPool;
    private final MemoryPool offHeapMemoryPool;
    private final long guaranteedMemory;

    @GuardedBy("this")
    private long spillUsed;

    public QueryContext(
            QueryId queryId,
            DataSize maxUserMemory,
            DataSize maxGpuMemory,
            DataSize maxOffHeapMemory,
            MemoryPool memoryPool,
            MemoryPool gpuDeviceMemoryPool,
            MemoryPool offHeapMemoryPool,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            ScheduledExecutorService timeoutExecutor,
            DataSize maxSpill,
            SpillSpaceTracker spillSpaceTracker)
    {
        this(queryId,
                maxUserMemory,
                maxGpuMemory,
                maxOffHeapMemory,
                memoryPool,
                gpuDeviceMemoryPool,
                offHeapMemoryPool,
                GUARANTEED_MEMORY,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                timeoutExecutor,
                maxSpill,
                spillSpaceTracker);
    }

    public QueryContext(
            QueryId queryId,
            DataSize maxUserMemory,
            DataSize maxGpuMemory,
            DataSize maxOffHeapMemory,
            MemoryPool memoryPool,
            MemoryPool gpuDeviceMemoryPool,
            MemoryPool offHeapMemoryPool,
            long guaranteedMemory,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            ScheduledExecutorService timeoutExecutor,
            DataSize maxSpill,
            SpillSpaceTracker spillSpaceTracker)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.maxUserMemory = maxUserMemory.toBytes();
        this.maxGpuMemory = requireNonNull(maxGpuMemory, "maxGpuMemory is null").toBytes();
        this.maxOffHeapMemory = requireNonNull(maxOffHeapMemory, "maxOffHeapMemory is null").toBytes();
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.gpuDeviceMemoryPool = requireNonNull(gpuDeviceMemoryPool, "gpuDeviceMemoryPool is null");
        this.offHeapMemoryPool = requireNonNull(offHeapMemoryPool, "offHeapMemoryPool is null");
        this.gcMonitor = requireNonNull(gcMonitor, "gcMonitor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.maxSpill = maxSpill.toBytes();
        this.spillSpaceTracker = requireNonNull(spillSpaceTracker, "spillSpaceTracker is null");
        this.guaranteedMemory = guaranteedMemory;
    }

    public boolean isMemoryLimitsInitialized()
    {
        return memoryLimitsInitialized;
    }

    // TODO: This method should be removed, and the correct limit set in the constructor. However, due to the way QueryContext is constructed the memory limit is not known in advance
    public synchronized void initializeMemoryLimits(boolean resourceOverCommit, long maxUserMemory)
    {
        checkArgument(maxUserMemory >= 0, "maxUserMemory must be >= 0, found: %s", maxUserMemory);
        if (resourceOverCommit) {
            // Allow the query to use the entire pool. This way the worker will kill the query, if it uses the entire local memory pool.
            // The coordinator will kill the query if the cluster runs out of memory.
            this.maxUserMemory = memoryPool.getMaxBytes();
        }
        else {
            this.maxUserMemory = maxUserMemory;
        }
        memoryLimitsInitialized = true;
    }

    @VisibleForTesting
    public synchronized long getMaxUserMemory()
    {
        return maxUserMemory;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    private synchronized ListenableFuture<Void> updateUserMemory(TaskId taskId, String allocationTag, long delta)
    {
        if (delta >= 0) {
            enforceUserMemoryLimit(memoryPool.getQueryMemoryReservation(queryId), delta, maxUserMemory);
            ListenableFuture<Void> future = memoryPool.reserve(taskId, allocationTag, delta);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }

            return future;
        }
        memoryPool.free(taskId, allocationTag, -delta);
        return NOT_BLOCKED;
    }

    // TODO Add tagging support for revocable memory reservations if needed
    private synchronized ListenableFuture<Void> updateRevocableMemory(TaskId taskId, long delta)
    {
        if (delta >= 0) {
            ListenableFuture<Void> future = memoryPool.reserveRevocable(taskId, delta);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }

            return future;
        }
        memoryPool.freeRevocable(taskId, -delta);
        return NOT_BLOCKED;
    }

    // TODO move spill tracking to the new memory tracking framework
    public synchronized ListenableFuture<Void> reserveSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (spillUsed + bytes > maxSpill) {
            throw exceededPerQueryLocalLimit(succinctBytes(maxSpill));
        }
        ListenableFuture<Void> future = spillSpaceTracker.reserve(bytes);
        spillUsed += bytes;
        return future;
    }

    private synchronized boolean tryUpdateUserMemory(TaskId taskId, String allocationTag, long delta)
    {
        if (delta <= 0) {
            ListenableFuture<Void> future = updateUserMemory(taskId, allocationTag, delta);
            // When delta == 0 and the pool is full the future can still not be done,
            // but, for negative deltas it must always be done.
            if (delta < 0) {
                verify(future.isDone(), "future should be done");
            }
            return true;
        }
        if (memoryPool.getQueryMemoryReservation(queryId) + delta > maxUserMemory) {
            return false;
        }
        return memoryPool.tryReserve(taskId, allocationTag, delta);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(spillUsed - bytes >= 0, "tried to free more memory than is reserved");
        spillUsed -= bytes;
        spillSpaceTracker.free(bytes);
    }

    private synchronized ListenableFuture<Void> updateGpuMemory(TaskId taskId, String allocationTag, long delta)
    {
        if (delta >= 0) {
            enforceGpuMemoryLimit(gpuDeviceMemoryPool.getQueryMemoryReservation(queryId), delta, maxGpuMemory);
            ListenableFuture<Void> future = gpuDeviceMemoryPool.reserve(taskId, allocationTag, delta);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }
        gpuDeviceMemoryPool.free(taskId, allocationTag, -delta);
        return NOT_BLOCKED;
    }

    private synchronized boolean tryUpdateGpuMemory(TaskId taskId, String allocationTag, long delta)
    {
        if (delta <= 0) {
            ListenableFuture<Void> future = updateGpuMemory(taskId, allocationTag, delta);
            if (delta < 0) {
                verify(future.isDone(), "future should be done");
            }
            return true;
        }
        if (gpuDeviceMemoryPool.getQueryMemoryReservation(queryId) + delta > maxGpuMemory) {
            return false;
        }
        return gpuDeviceMemoryPool.tryReserve(taskId, allocationTag, delta);
    }

    private synchronized ListenableFuture<Void> updateOffHeapMemory(TaskId taskId, String allocationTag, long delta)
    {
        if (delta >= 0) {
            enforceOffHeapMemoryLimit(offHeapMemoryPool.getQueryMemoryReservation(queryId), delta, maxOffHeapMemory);
            ListenableFuture<Void> future = offHeapMemoryPool.reserve(taskId, allocationTag, delta);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }
        offHeapMemoryPool.free(taskId, allocationTag, -delta);
        return NOT_BLOCKED;
    }

    private synchronized boolean tryUpdateOffHeapMemory(TaskId taskId, String allocationTag, long delta)
    {
        if (delta <= 0) {
            ListenableFuture<Void> future = updateOffHeapMemory(taskId, allocationTag, delta);
            if (delta < 0) {
                verify(future.isDone(), "future should be done");
            }
            return true;
        }
        if (offHeapMemoryPool.getQueryMemoryReservation(queryId) + delta > maxOffHeapMemory) {
            return false;
        }
        return offHeapMemoryPool.tryReserve(taskId, allocationTag, delta);
    }

    public MemoryPool getMemoryPool()
    {
        return memoryPool;
    }

    public long getUserMemoryReservation()
    {
        return memoryPool.getQueryMemoryReservation(queryId);
    }

    public TaskContext addTaskContext(
            TaskStateMachine taskStateMachine,
            Map<PlanNodeId, ConnectorTableCredentials> tableCredentials,
            Session session,
            Runnable notifyStatusChanged,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled)
    {
        TaskId taskId = taskStateMachine.getTaskId();

        // Note that task user memory cannot be closed even when TaskStateMachine reaches a terminal state.
        // Task output buffers may be still live and waiting for consumer to release them.
        AggregatedMemoryContext taskUserMemory = newRootAggregatedMemoryContext(
                new QueryMemoryReservationHandler(
                        (tag, delta) -> updateUserMemory(taskId, tag, delta),
                        (tag, delta) -> tryUpdateUserMemory(taskId, tag, delta),
                        transferTagsFunction(memoryPool, taskId)),
                guaranteedMemory);
        AggregatedMemoryContext taskRevocableMemory = newRootAggregatedMemoryContext(
                new QueryMemoryReservationHandler(
                        (_, delta) -> updateRevocableMemory(taskId, delta),
                        (_, _) -> tryReserveMemoryNotSupported(),
                        (_, _, _) -> {
                            throw new UnsupportedOperationException("Revocable memory allocations are untagged and cannot be transferred");
                        }),
                0L);
        AggregatedMemoryContext taskGpuDeviceMemory = newRootAggregatedMemoryContext(
                new QueryMemoryReservationHandler(
                        (tag, delta) -> updateGpuMemory(taskId, tag, delta),
                        (tag, delta) -> tryUpdateGpuMemory(taskId, tag, delta),
                        transferTagsFunction(gpuDeviceMemoryPool, taskId)),
                0L);
        AggregatedMemoryContext taskOffHeapMemory = newRootAggregatedMemoryContext(
                new QueryMemoryReservationHandler(
                        (tag, delta) -> updateOffHeapMemory(taskId, tag, delta),
                        (tag, delta) -> tryUpdateOffHeapMemory(taskId, tag, delta),
                        transferTagsFunction(offHeapMemoryPool, taskId)),
                0L);
        MemoryTrackingContext taskMemoryContext = new MemoryTrackingContext(taskUserMemory, taskRevocableMemory);
        GpuTaskMemoryContext gpuTaskMemoryContext = new GpuTaskMemoryContext(taskUserMemory, taskGpuDeviceMemory, taskOffHeapMemory);

        taskStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                // taskUserMemory cannot be closed, see comment above
                long gpuDeviceMemoryBytes = taskGpuDeviceMemory.getBytes();
                if (gpuDeviceMemoryBytes != 0) {
                    log.warn(
                            "Task %s reached state %s but still holds %s GPU memory. Query allocations: %s",
                            taskId,
                            state,
                            succinctBytes(gpuDeviceMemoryBytes),
                            getAdditionalFailureInfo(gpuDeviceMemoryPool, gpuDeviceMemoryPool.getQueryMemoryReservation(queryId), 0));
                }
                taskGpuDeviceMemory.close();

                long offHeapMemoryBytes = taskOffHeapMemory.getBytes();
                if (offHeapMemoryBytes != 0) {
                    log.warn(
                            "Task %s reached state %s but still holds %s off-heap memory. Query allocations: %s",
                            taskId,
                            state,
                            succinctBytes(offHeapMemoryBytes),
                            getAdditionalFailureInfo(offHeapMemoryPool, offHeapMemoryPool.getQueryMemoryReservation(queryId), 0));
                }
                taskOffHeapMemory.close();
            }
        });

        TaskContext taskContext = createTaskContext(
                this,
                taskStateMachine,
                tableCredentials,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                timeoutExecutor,
                session,
                taskMemoryContext,
                gpuTaskMemoryContext,
                notifyStatusChanged,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled);
        taskContexts.put(taskId, taskContext);
        return taskContext;
    }

    /**
     * Creates a memory context for split enumeration this node performs for the query outside any
     * task (remote split tasks). Reservations land in the query's user memory on this node — counted
     * against the per-node limit and attributed to the query in the memory pool — under the given
     * synthetic task id, whose only significant part is the query id.
     */
    public LocalMemoryContext addSplitSourceMemoryContext(TaskId taskId, String tag)
    {
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(
                new QueryMemoryReservationHandler(
                        (allocationTag, delta) -> updateUserMemory(taskId, allocationTag, delta),
                        (allocationTag, delta) -> tryUpdateUserMemory(taskId, allocationTag, delta),
                        transferTagsFunction(memoryPool, taskId)),
                0L);
        return memoryContext.newLocalMemoryContext(tag);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitQueryContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return taskContexts.values()
                .stream()
                .map(taskContext -> taskContext.accept(visitor, context))
                .collect(toList());
    }

    public TaskContext getTaskContextByTaskId(TaskId taskId)
    {
        TaskContext taskContext = taskContexts.get(taskId);
        return verifyNotNull(taskContext, "task does not exist");
    }

    private static class QueryMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private final BiFunction<String, Long, ListenableFuture<Void>> reserveMemoryFunction;
        private final BiPredicate<String, Long> tryReserveMemoryFunction;
        private final TagTransferFunction tagTransferFunction;

        public QueryMemoryReservationHandler(
                BiFunction<String, Long, ListenableFuture<Void>> reserveMemoryFunction,
                BiPredicate<String, Long> tryReserveMemoryFunction,
                TagTransferFunction tagTransferFunction)
        {
            this.reserveMemoryFunction = requireNonNull(reserveMemoryFunction, "reserveMemoryFunction is null");
            this.tryReserveMemoryFunction = requireNonNull(tryReserveMemoryFunction, "tryReserveMemoryFunction is null");
            this.tagTransferFunction = requireNonNull(tagTransferFunction, "tagTransferFunction is null");
        }

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            return reserveMemoryFunction.apply(allocationTag, delta);
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            return tryReserveMemoryFunction.test(allocationTag, delta);
        }

        @Override
        public void transferTags(String fromTag, String toTag, long bytes)
        {
            tagTransferFunction.transfer(fromTag, toTag, bytes);
        }
    }

    private TagTransferFunction transferTagsFunction(MemoryPool memoryPool, TaskId taskId)
    {
        return (fromTag, toTag, bytes) -> memoryPool.transferTags(taskId, fromTag, toTag, bytes);
    }

    @FunctionalInterface
    interface TagTransferFunction
    {
        void transfer(String fromTag, String toTag, long bytes);
    }

    private boolean tryReserveMemoryNotSupported()
    {
        throw new UnsupportedOperationException("tryReserveMemory is not supported");
    }

    @GuardedBy("this")
    private void enforceUserMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalUserMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(memoryPool, allocated, delta));
        }
    }

    @GuardedBy("this")
    private void enforceGpuMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalGpuMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(gpuDeviceMemoryPool, allocated, delta));
        }
    }

    @GuardedBy("this")
    private void enforceOffHeapMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalOffHeapMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(offHeapMemoryPool, allocated, delta));
        }
    }

    private String getAdditionalFailureInfo(MemoryPool pool, long allocated, long delta)
    {
        Map<String, Long> queryAllocations = pool.getTaggedMemoryAllocations(queryId);

        String additionalInfo = format("Allocated: %s, Delta: %s", succinctBytes(allocated), succinctBytes(delta));

        // It's possible that a query tries allocating more than the available memory
        // failing immediately before any allocation of that query is tagged
        if (queryAllocations.isEmpty()) {
            return additionalInfo;
        }

        String topConsumers = queryAllocations.entrySet().stream()
                .sorted(comparingByValue(Comparator.reverseOrder()))
                .limit(3)
                .filter(e -> e.getValue() >= 0)
                .collect(toImmutableMap(Entry::getKey, e -> succinctBytes(e.getValue())))
                .toString();

        return format("%s, Top Consumers: %s", additionalInfo, topConsumers);
    }
}
