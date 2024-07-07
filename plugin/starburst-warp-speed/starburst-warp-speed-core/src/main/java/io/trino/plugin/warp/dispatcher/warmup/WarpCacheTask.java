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
package io.trino.plugin.warp.dispatcher.warmup;

import io.airlift.log.Logger;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.cache.CacheAction;
import io.trino.plugin.warp.dispatcher.cache.MemoryContextService;
import io.trino.plugin.warp.dispatcher.cache.WarmupElementBlocks;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.flows.FlowIdGenerator;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.storage.write.WarmResult;
import io.trino.plugin.warp.storage.write.WarmupCacheData;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE_FD;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FLOW_ID;
import static java.util.Objects.requireNonNull;

public class WarpCacheTask
        implements WorkerSubmittableTask
{
    private static final Logger logger = Logger.get(WarpCacheTask.class);
    private final ShapingLogger shapingLogger;
    private static final int STOP_TRIGGER = -1;
    private final CacheWarmer cacheWarmer;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final WarmingServiceStats statsWarmingService;
    private final StorageWarmerService storageWarmerService;
    private final List<WarmupElementWriteMetadata> toWarm;
    private final Map<CacheWarmState, CacheAction> cacheActions;
    private final MemoryContextService memoryContextService;
    private final WarmupCacheData warmupCacheData;
    private final RowGroupKey rowGroupKey;
    private final UUID id;
    boolean warmStarted;
    private StorageWriterSplitConfig storageWriterSplitConfig;
    private List<WarmingCandidate> warmingCandidates;
    private int totalRecords;

    private boolean warpAbort;
    private long flowId = -1;
    private LocalMemoryContext localMemoryContext;
    private final BlockingDeque<Integer> blocksToProcess;
    private boolean engineAbort;
    private boolean taskStarted;
    private boolean finished;
    private boolean revoked;

    public WarpCacheTask(GlobalConfig globalConfig,
            Map<CacheWarmState, CacheAction> cacheActions,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService,
            WarmupCacheData warmupCacheData,
            CacheWarmer cacheWarmer,
            WarmingServiceStats statsWarmingService,
            List<WarmupElementWriteMetadata> toWarm,
            RowGroupKey rowGroupKey,
            MemoryContextService memoryContextService)
    {
        this.warmupCacheData = warmupCacheData;
        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.statsWarmingService = requireNonNull(statsWarmingService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.toWarm = requireNonNull(toWarm);
        this.cacheActions = requireNonNull(cacheActions);
        this.memoryContextService = requireNonNull(memoryContextService);
        this.id = UUID.randomUUID();
        this.warpAbort = false;
        this.engineAbort = false;
        this.blocksToProcess = new LinkedBlockingDeque<>();
        this.taskStarted = false;
        this.revoked = false;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    @Override
    public UUID getId()
    {
        return id;
    }

    @Override
    public int getPriority()
    {
        return WorkerTaskExecutorService.TaskExecutionType.CACHE.getPriority();
    }

    @Override
    public RowGroupKey getRowGroupKey()
    {
        return rowGroupKey;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmingService.incwarm_scheduled();
    }

    @Override
    public void run()
    {
        taskStarted = true;
        CacheWarmState cacheWarmState = CacheWarmState.ABORT_ON_INIT_PROCESS;
        boolean loadFromWarmingThread = false;
        if (revoked) {
            //all resources already released by @revoke
            return;
        }
        if (isAborted()) {
            workerTaskExecutorService.taskFinished(rowGroupKey);
            memoryContextService.remove(this);
            return;
        }
        try {
            try {
                loadFromWarmingThread = storageWarmerService.isLoaderAvailable();
                if (!loadFromWarmingThread) {
                    storageWarmerService.waitForLoaders();
                }
                statsWarmingService.incwarm_started();
                if (isAborted()) {
                    return;
                }
                cacheWarmState = init();
                if (isEngineAbort()) {
                    cacheWarmState = CacheWarmState.ABORT_FROM_ENGINE;
                    return;
                }
                if (cacheWarmState == CacheWarmState.RUNNING) {
                    cacheWarmState = processAll();
                }
            }
            catch (InterruptedException e) {
                setWarpAbort();
            }
            finally {
                closeAndSave(cacheWarmState);
            }
        }
        finally {
            try {
                cacheWarmer.finishWarmingAndUnlock(storageWriterSplitConfig, rowGroupKey);
            }
            catch (Exception e) {
                logger.error(e, "failed on finish cache warming %s. key=%s", storageWriterSplitConfig, rowGroupKey);
            }
            if (localMemoryContext != null) {
                memoryContextService.releaseMemory(localMemoryContext);
            }
            if (loadFromWarmingThread) {
                storageWarmerService.releaseLoaderThread(true);
            }
            workerTaskExecutorService.taskFinished(rowGroupKey);
            memoryContextService.remove(this);
            statsWarmingService.incwarm_finished();
        }
    }

    private CacheWarmState processAll()
    {
        CacheWarmState cacheWarmState = CacheWarmState.RUNNING;

        try {
            while (!isAborted()) {
                int blockIndexToProcess = blocksToProcess.take();
                if (isAborted() || blockIndexToProcess == STOP_TRIGGER) {
                    break;
                }
                WarmupElementBlocks warmupElementBlocks = warmupCacheData.getWarmupElementBlock(blockIndexToProcess);
                if (warmupElementBlocks.isEmpty()) { // in case it was added to the queue and then finished() was called and caused it to be added again
                    continue;
                }
                if (finished || warmupElementBlocks.isReady()) {
                    processBlock(warmupElementBlocks, blockIndexToProcess);
                }
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to warm cache elements");
            cacheWarmState = CacheWarmState.ABORTING;
            setWarpAbort();
        }
        if (!isAborted() && warmupCacheData.notAllDataFlushed()) {
            // This shouldn't happen. The check is for safety, so we won't get wrong results on query
            shapingLogger.error("There's a bug - Not all blocks were fully written - aborting. blocksToProcess=%s, warmupCacheData=%s", blocksToProcess.toString(), warmupCacheData);
            cacheWarmState = CacheWarmState.ABORTING;
            setWarpAbort();
        }
        if (cacheWarmState == CacheWarmState.RUNNING) {
            if (isEngineAbort()) {
                cacheWarmState = CacheWarmState.ABORT_FROM_ENGINE;
            }
            else if (isWarpAbort()) {
                cacheWarmState = CacheWarmState.ABORTING;
            }
            else {
                cacheWarmState = CacheWarmState.FINISHING;
            }
        }
        return cacheWarmState;
    }

    private void processBlock(WarmupElementBlocks warmupElementBlocks, int blockIndexToProcess)
    {
        WarmingCandidate warmingCandidate = warmingCandidates.get(blockIndexToProcess);

        WarmResult result = warmingCandidate.pageSink().appendWarmupElementBlocks(warmupElementBlocks);
        if (result.success()) {
            warmupElementBlocks.dropProcessed(result.columnBlockIndex(), result.offset());
            if (warmupElementBlocks.isReady()) {
                // there's still work to do (add first for the case STOP_TRIGGER was already added)
                blocksToProcess.addFirst(blockIndexToProcess);
            }
        }
        else {
            // failedElement = Optional.of(warmupElementBlocks.getMetadata()); // TODO: The one that failed should be marked as temporary failed
            setWarpAbort();
        }
    }

    public void warmAsEmptyPageSource()
    {
        statsWarmingService.incwarm_started();
        warmingCandidates = toWarm.stream().map(x -> new WarmingCandidate(new long[] {INVALID_FILE_COOKIE_FD, 0}, null, 0, x, null)).collect(Collectors.toList());
        CacheWarmState cacheWarmState = CacheWarmState.EMPTY_PAGE;
        closeAndSave(cacheWarmState);
        memoryContextService.remove(this);
    }

    private void closeAndSave(CacheWarmState cacheWarmState)
    {
        CacheAction cacheAction = cacheActions.get(cacheWarmState);
        try {
            cacheWarmState = cacheAction.act(warmingCandidates, totalRecords, rowGroupKey);
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to finish handle cache key=%s", rowGroupKey);
            cacheWarmState = CacheWarmState.ABORTING;  // so we'll clean the storage
        }
        finally {
            try {
                cacheAction = cacheActions.get(cacheWarmState);
                boolean warmSucceeded = cacheAction.close(warmingCandidates,
                        rowGroupKey,
                        flowId,
                        storageWriterSplitConfig);
                if (warmSucceeded) {
                    statsWarmingService.incwarm_accomplished();
                    statsWarmingService.incwarm_warp_cache_accomplished();
                }
                else if (engineAbort) {
                    statsWarmingService.incwarm_warp_cache_engine_aborted();
                }
                else {
                    // Note: warm_failed is per WarmUpElement and increased when necessary at CacheAction#cleanStorage
                    statsWarmingService.incwarm_warp_cache_failed();
                }
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to clean storage for cache key=%s", rowGroupKey);
            }
        }
    }

    private CacheWarmState init()
            throws InterruptedException
    {
        if (!initWarmUpProcess()) {
            return CacheWarmState.ABORT_ON_INIT_PROCESS;
        }

        storageWriterSplitConfig = cacheWarmer.lockAndStartWarming(rowGroupKey);
        warmStarted = true;

        return initCandidates() ? CacheWarmState.RUNNING : CacheWarmState.ABORTING;
    }

    private boolean initWarmUpProcess()
    {
        boolean success = false;
        statsWarmingService.incwarm_warp_cache_started();
        try {
            flowId = FlowIdGenerator.generateFlowId();
            storageWarmerService.tryRunningWarmFlow(flowId, rowGroupKey);
            success = true;
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to init warm up process. key=%s", rowGroupKey);
            flowId = INVALID_FLOW_ID;
        }
        return success;
    }

    private boolean initCandidates()
    {
        boolean success = true;
        warmingCandidates = new ArrayList<>(toWarm.size());
        for (WarmupElementWriteMetadata warmUpElementToWarm : toWarm) {
            RowGroupKey tmpRowGroupKey = cacheWarmer.getTempRowGroupKey(warmUpElementToWarm, rowGroupKey);
            try {
                WarmingCandidate warmingCandidate = cacheWarmer.initCandidate(storageWriterSplitConfig, warmUpElementToWarm, tmpRowGroupKey);
                warmingCandidates.add(warmingCandidate);
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to init warm candidate key=%s. %s", tmpRowGroupKey, warmUpElementToWarm);
                // failedElement = Optional.of(warmUpElementToWarm); // TODO: The one that failed should be marked as temporary failed
                setWarpAbort();
                success = false;
                break;
            }
        }
        return success;
    }

    public long addPage(Page page)
    {
        if (isAborted()) {
            logger.debug("add page in failed state, do nothing");
            return 0;
        }
        if (finished) {
            String error = String.format("Received a page while in finished state. rowGroupKey=%s", rowGroupKey);
            shapingLogger.error(error);
            setWarpAbort();
            return 0;
        }
        if ((long) totalRecords + (long) page.getPositionCount() >= Integer.MAX_VALUE) {
            logger.debug("total record is bigger than Integer.MAX_VALUE. not supported");
            setWarpAbort();
            return 0;
        }
        totalRecords += page.getPositionCount();
        for (int blockIndex = 0; blockIndex < page.getChannelCount(); blockIndex++) {
            Block block = page.getBlock(blockIndex);
            boolean isReady = warmupCacheData.addBlock(block, blockIndex);
            if (isReady) {
                blocksToProcess.add(blockIndex);
            }
        }
        return warmupCacheData.getRetainedSizeInBytes();
    }

    public boolean isWarmStarted()
    {
        return warmStarted;
    }

    public void revoke()
    {
        revoked = true;
        setEngineAbort();
        blocksToProcess.clear();
        blocksToProcess.add(STOP_TRIGGER);
        warmupCacheData.clear();
        memoryContextService.releaseMemory(localMemoryContext);
        localMemoryContext = null; //we set to null in case it started, so we won't release twice
        memoryContextService.remove(this);
        workerTaskExecutorService.taskFinished(rowGroupKey);
    }

    public long getRetainedSizeInBytes()
    {
        return warmupCacheData.getRetainedSizeInBytes();
    }

    public synchronized void setFinished()
    {
        if (isAborted()) {
            return;
        }
        this.localMemoryContext = memoryContextService.poll();
        if (localMemoryContext == null) {
            shapingLogger.error("failed to locate memory context size=%s", memoryContextService.getRunningSize());
            if (!taskStarted) {
                shapingLogger.info("clear memory of Task since it not started yet but hold memory");
                warmupCacheData.clear();
            }
            setEngineAbort();
        }
        else if (localMemoryContext.trySetBytes(warmupCacheData.getRetainedSizeInBytes())) {
            finished = true;
            for (int i = 0; i < toWarm.size(); i++) {
                blocksToProcess.add(i);
            }
            blocksToProcess.add(STOP_TRIGGER);
        }
        else {
            logger.debug("too much memory allocated in cache. currentTaskSize=%s, rowGroupKey=%s", warmupCacheData.getRetainedSizeInBytes(), rowGroupKey);
            setEngineAbort();
        }
    }

    private synchronized void setWarpAbort()
    {
        if (isAborted()) {
            return;
        }
        this.warpAbort = true;
        blocksToProcess.add(STOP_TRIGGER);
    }

    public synchronized void setEngineAbort()
    {
        if (isAborted()) {
            logger.info("already aborted but got abort again");
            return;
        }
        this.engineAbort = true;
        blocksToProcess.add(STOP_TRIGGER);
    }

    private boolean isWarpAbort()
    {
        return warpAbort;
    }

    private boolean isEngineAbort()
    {
        return engineAbort;
    }

    private synchronized boolean isAborted()
    {
        return warpAbort || engineAbort;
    }

    public long getUsedMemory()
    {
        return warmupCacheData.getRetainedSizeInBytes();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowGroupKey);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarpCacheTask rowGroupData = (WarpCacheTask) o;
        return Objects.equals(rowGroupKey, rowGroupData.rowGroupKey);
    }

    @Override
    public String toString()
    {
        return "WarpCacheTask{" +
                "rowGroupKey=" + rowGroupKey +
                '}';
    }
}
