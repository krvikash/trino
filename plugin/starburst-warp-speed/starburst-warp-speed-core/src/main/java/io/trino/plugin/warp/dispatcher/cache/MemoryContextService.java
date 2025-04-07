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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.WarpCacheTask;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.MemoryAllocator;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;

@Singleton
public class MemoryContextService
{
    private static final Logger logger = Logger.get(MemoryContextService.class);
    private final LinkedBlockingQueue<LocalMemoryContext> localMemoryContexts;
    private final WarmingServiceStats statsWarmingService;
    private final ShapingLogger shapingLogger;
    private boolean revokeIsRunning;
    private final MemoryAllocator revocableMemoryAllocator;
    private long allocatedMemory;

    private static final int LOCAL_MEMORY_COUNT = 1500;
    private final Set<WarpCacheTask> runningTasks;

    @Inject
    public MemoryContextService(CacheManagerContext cacheManagerContext,
            MetricsManager metricsManager,
            WarmupDemoterConfig warmupDemoterConfig,
            ShapingLoggerFactory shapingLoggerFactory)
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
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create());
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    public LocalMemoryContext poll()
    {
        return localMemoryContexts.poll();
    }

    public void releaseMemory(LocalMemoryContext localMemoryContext)
    {
        try {
            //localMemoryContext is null when revoke triggered before setting localMemoryContext
            if (localMemoryContext != null) {
                localMemoryContext.trySetBytes(0); //warming finished
                localMemoryContexts.put(localMemoryContext);
            }
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
            shapingLogger.error("failed to remove task from running tasks. runningTasksSize=%s error=%s", runningTasks.size(), e.getMessage());
        }
    }

    public boolean add(WarpCacheTask warpCacheTask)
    {
        if (localMemoryContexts.isEmpty()) {
            shapingLogger.info("localMemoryContexts is empty. LOCAL_MEMORY_COUNT=%s runningTasks.size()=%s", LOCAL_MEMORY_COUNT, runningTasks.size());
            return false;
        }
        return runningTasks.add(warpCacheTask);
    }

    public long revoke(long bytesToRevoke)
    {
        long revokedMemory = 0;
        try {
            if (runningTasks.isEmpty()) {
                logger.debug("revoke %s bytes triggered but WarpCacheManager doesn't have any running warming tasks. allocatedMemory=%s, localMemoryContextsSize=%s. ignoring this event",
                        bytesToRevoke, getAllocatedMemory(), localMemoryContexts.size());
                return 0;
            }
            shapingLogger.info("revoke memory triggered bytesToRevoke=%s, allocatedMemory=%s, runningTasksSize=%s, localMemoryContexts.size()=%s", bytesToRevoke, getAllocatedMemory(), getRunningSize(), localMemoryContexts.size());
            statsWarmingService.incwarm_warp_cache_revoke_started();
            revokeIsRunning = true;
            int iteration = 0;
            while (revokedMemory < bytesToRevoke && !runningTasks.isEmpty()) {
                //better to pick tasks that is still running and not to interrupt current warming tasks
                Optional<WarpCacheTask> warpCacheTaskOpt = runningTasks.stream().filter(x -> !x.isWarmStarted() && !x.isRevoked()).max(Comparator.comparingLong(WarpCacheTask::getRetainedSizeInBytes));
                if (warpCacheTaskOpt.isEmpty()) {
                    warpCacheTaskOpt = runningTasks.stream().filter(x -> !x.isRevoked()).max(Comparator.comparingLong(WarpCacheTask::getRetainedSizeInBytes));
                }
                if (warpCacheTaskOpt.isEmpty()) {
                    //protect a race in case another thread poll a task
                    logger.debug("running task is not empty but all elements are revoked. break runningTaskSize=%s, they should be clean by WarpCacheTask flow", getRunningSize());
                    break;
                }
                WarpCacheTask warpCacheTask = warpCacheTaskOpt.get();
                revokedMemory += warpCacheTask.getUsedMemory();
                warpCacheTask.revoke();
                iteration++;
            }
            shapingLogger.info("revoked memory=%s of bytesToRevoke=%s, ,allocatedMemory=%s, runningTasksSize=%s, totalRevokedTasks=%s, localMemoryContexts.size()=%s", revokedMemory, bytesToRevoke, getAllocatedMemory(), runningTasks.size(), iteration, localMemoryContexts.size());
            statsWarmingService.incwarm_warp_cache_revoke_accomplished();
            statsWarmingService.addwarm_warp_cache_revoked_bytes(revokedMemory);
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to revoke");
            statsWarmingService.incwarm_warp_cache_revoke_failed();
        }
        finally {
            revokeIsRunning = false;
        }
        return revokedMemory;
    }

    private long getAllocatedMemory()
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
                logger.debug("delta is 0");
                // noop
                return true;
            }

            synchronized (MemoryContextService.this) {
                if (!revocableMemoryAllocator.trySetBytes(allocatedMemory + delta)) {
                    return false;
                }
                allocatedMemory += delta;
                return true;
            }
        }
    }
}
