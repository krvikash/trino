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

package io.trino.plugin.warp.dispatcher.cache;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.WarpCacheTask;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.MemoryAllocator;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;

@Singleton
public class MemoryContextService
{
    private static final Logger logger = Logger.get(MemoryContextService.class);
    private final LinkedBlockingQueue<LocalMemoryContext> localMemoryContexts;
    private final WarmingServiceStats statsWarmingService;
    private boolean revokeIsRunning;
    private final MemoryAllocator revocableMemoryAllocator;
    @GuardedBy("this")
    private long allocatedMemory;

    private static final int LOCAL_MEMORY_COUNT = 1500;
    private final Set<WarpCacheTask> runningTasks;

    @Inject
    public MemoryContextService(CacheManagerContext cacheManagerContext,
            MetricsManager metricsManager,
            WarmupDemoterConfig warmupDemoterConfig)
    {
        int queueSize = warmupDemoterConfig.getTasksExecutorQueueSize();
        this.localMemoryContexts = new LinkedBlockingQueue<>(queueSize);
        this.runningTasks = Sets.newConcurrentHashSet();
        this.revokeIsRunning = false;
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(new WarpCacheMemoryReservationHandler(), 0L);
        this.revocableMemoryAllocator = cacheManagerContext.revocableMemoryAllocator();
        for (int i = 0; i < queueSize; i++) {
            LocalMemoryContext localMemoryContext = memoryContext.newLocalMemoryContext("ignored");
            localMemoryContexts.add(localMemoryContext);
        }
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create(WARMING_SERVICE_STAT_GROUP));
    }

    public LocalMemoryContext poll()
    {
        return localMemoryContexts.poll();
    }

    public void releaseMemory(LocalMemoryContext localMemoryContext)
    {
        try {
            localMemoryContext.trySetBytes(0); //warming finished
            localMemoryContexts.put(localMemoryContext);
        }
        catch (Exception e) {
            logger.error("failed to put localMemoryContext localMemoryContextsSize=%s error=%s", localMemoryContexts.size(), e.getMessage());
        }
    }

    public void remove(WarpCacheTask warpCacheTask)
    {
        try {
            runningTasks.remove(warpCacheTask);
        }
        catch (Exception e) {
            logger.error("failed to remove task from running tasks. runningTasksSize=%s error=%s", runningTasks.size(), e.getMessage());
        }
    }

    public boolean add(WarpCacheTask warpCacheTask)
    {
        if (localMemoryContexts.isEmpty()) {
            logger.info("localMemoryContexts is empty. LOCAL_MEMORY_COUNT=%s runningTasks.size()=%s, localMemoryContexts.size()=%s", LOCAL_MEMORY_COUNT, runningTasks.size(), localMemoryContexts.size());
            return false;
        }
        return runningTasks.add(warpCacheTask);
    }

    public long revoke(long bytesToRevoke)
    {
        logger.info("revoke memory triggered bytesToRevoke=%s, allocatedMemory=%s, runningTasksSize=%s, localMemoryContexts.size()=%s", bytesToRevoke, getAllocatedMemory(), runningTasks.size(), localMemoryContexts.size());
        long revokedMemory = 0;
        try {
            statsWarmingService.incwarm_warp_cache_revoke_started();
            revokeIsRunning = true;
            while (revokedMemory < bytesToRevoke && !runningTasks.isEmpty()) {
                Optional<WarpCacheTask> warpCacheTaskOpt = runningTasks.stream().findAny();
                if (warpCacheTaskOpt.isEmpty()) {
                    //protect a race in case another thread poll a task
                    continue;
                }
                WarpCacheTask warpCacheTask = warpCacheTaskOpt.get();
                revokedMemory += warpCacheTask.getUsedMemory();
                if (warpCacheTask.isWarmStarted()) {
                    warpCacheTask.setEngineAbort();
                }
                else {
                    logger.info("revoke cache manager: warp warm not started yet, we set state as abort and release the memory");
                    warpCacheTask.revoke();
                }
                logger.info("revoked memory=%s of bytesToRevoke=%s, runningTasksSize=%s", revokedMemory, bytesToRevoke, runningTasks.size());
            }
            statsWarmingService.incwarm_warp_cache_revoke_accomplished();
        }
        catch (Exception e) {
            logger.error(e, "failed to revoke");
            statsWarmingService.incwarm_warp_cache_revoke_failed();
        }
        finally {
            revokeIsRunning = false;
        }
        return revokedMemory;
    }

    private synchronized long getAllocatedMemory()
    {
        return allocatedMemory;
    }

    public int getRunningSize()
    {
        return runningTasks.size();
    }

    public boolean revokeIsRunning()
    {
        return revokeIsRunning;
    }

    private class WarpCacheMemoryReservationHandler
            implements MemoryReservationHandler
    {
        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            throw new IllegalStateException();
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            if (delta == 0) {
                logger.info("delta is 0");
                // noop
                return true;
            }

            synchronized (MemoryContextService.this) {
                if (!revocableMemoryAllocator.trySetBytes(allocatedMemory + delta)) {
                    logger.info("failed to locate WarpCacheManager memory. delta=%s, allocatedMemory=%s", delta, allocatedMemory);
                    return false;
                }
                allocatedMemory += delta;
                logger.debug("allocatedMemory=%s delta=%s", allocatedMemory, delta);
                return true;
            }
        }
    }
}
