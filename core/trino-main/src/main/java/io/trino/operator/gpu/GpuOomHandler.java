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

import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmEventHandler;
import com.google.common.collect.ImmutableMap;
import io.trino.memory.MemoryPool;
import io.trino.spi.QueryId;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Map.Entry.comparingByValue;
import static java.util.Objects.requireNonNull;

/// Captures GPU memory diagnostics at the point of allocation failure (OOM).
/// By the time [GpuOperator]'s catch block handles the error, memory may have already been released
/// during stack unwinding, making the pool state misleading.
public final class GpuOomHandler
        implements RmmEventHandler
{
    private static final ThreadLocal<FailureSnapshot> LAST_FAILURE_INFO = new ThreadLocal<>();
    private static final ThreadLocal<QueryId> CURRENT_QUERY = new ThreadLocal<>();

    private final AtomicReference<MemoryPool> gpuDeviceMemoryPool = new AtomicReference<>();

    void setGpuDeviceMemoryPool(MemoryPool pool)
    {
        checkState(gpuDeviceMemoryPool.compareAndSet(null, pool), "gpuDeviceMemoryPool already set");
    }

    public static void setContext(QueryId queryId)
    {
        CURRENT_QUERY.set(queryId);
        LAST_FAILURE_INFO.remove();
    }

    public static void clearContext()
    {
        CURRENT_QUERY.remove();
        LAST_FAILURE_INFO.remove();
    }

    public static Optional<FailureSnapshot> getLastFailureSnapshot()
    {
        return Optional.ofNullable(LAST_FAILURE_INFO.get());
    }

    @Override
    public boolean onAllocFailure(long sizeRequested, int retryCount)
    {
        MemoryPool pool = gpuDeviceMemoryPool.get();
        QueryId queryId = CURRENT_QUERY.get();
        if (pool != null && queryId != null) {
            LAST_FAILURE_INFO.set(captureSnapshot(pool, sizeRequested, queryId));
        }
        return false;
    }

    private static FailureSnapshot captureSnapshot(MemoryPool pool, long sizeRequested, QueryId currentQueryId)
    {
        long rmmAllocated = Rmm.getTotalBytesAllocated();
        long poolMaxBytes = pool.getMaxBytes();
        long poolReservedBytes = pool.getReservedBytes();
        Map<QueryId, Long> queryReservations = pool.getQueryMemoryReservations();

        long currentQueryReservation = 0;
        Map<String, Long> currentQueryTags = ImmutableMap.of();
        if (currentQueryId != null) {
            currentQueryReservation = queryReservations.getOrDefault(currentQueryId, 0L);
            currentQueryTags = pool.getTaggedMemoryAllocations(currentQueryId);
        }

        return new FailureSnapshot(
                sizeRequested,
                poolMaxBytes,
                poolReservedBytes,
                rmmAllocated,
                currentQueryReservation,
                currentQueryTags,
                queryReservations.size(),
                currentQueryId);
    }

    public record FailureSnapshot(
            long sizeRequested,
            long poolMaxBytes,
            long poolReservedBytes,
            long rmmAllocatedBytes,
            long currentQueryReservation,
            Map<String, Long> currentQueryTaggedAllocations,
            int totalQueryCount,
            QueryId currentQueryId)
    {
        public FailureSnapshot
        {
            currentQueryTaggedAllocations = ImmutableMap.copyOf(requireNonNull(currentQueryTaggedAllocations, "currentQueryTaggedAllocations is null"));
            requireNonNull(currentQueryId, "currentQueryId is null");
        }

        public String toErrorMessage()
        {
            String topConsumers = currentQueryTaggedAllocations.entrySet().stream()
                    .sorted(comparingByValue(Comparator.reverseOrder()))
                    .limit(3)
                    .filter(e -> e.getValue() >= 0)
                    .collect(toImmutableMap(Map.Entry::getKey, e -> succinctBytes(e.getValue())))
                    .toString();
            String failureInfo = "Failed to allocate %s. GPU memory pool: %s total, %s reserved, %s allocated by RMM. This query (%s): %s reserved; top consumers: %s.".formatted(
                    succinctBytes(sizeRequested),
                    succinctBytes(poolMaxBytes),
                    succinctBytes(poolReservedBytes),
                    succinctBytes(rmmAllocatedBytes),
                    currentQueryId,
                    succinctBytes(currentQueryReservation),
                    topConsumers);
            int otherQueryCount = totalQueryCount - 1;
            if (otherQueryCount > 0) {
                long otherReserved = poolReservedBytes - currentQueryReservation;
                return failureInfo + " Other queries: %s reserved across %s queries.".formatted(succinctBytes(otherReserved), otherQueryCount);
            }
            return failureInfo;
        }
    }

    @Override
    public long[] getAllocThresholds()
    {
        return null;
    }

    @Override
    public long[] getDeallocThresholds()
    {
        return null;
    }

    @Override
    public void onAllocThreshold(long totalAllocSize) {}

    @Override
    public void onDeallocThreshold(long totalAllocSize) {}
}
