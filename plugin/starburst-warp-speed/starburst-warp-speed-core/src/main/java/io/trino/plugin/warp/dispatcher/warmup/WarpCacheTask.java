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
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.cache.CacheAction;
import io.trino.plugin.warp.dispatcher.cache.CacheWarmupElementArgs;
import io.trino.plugin.warp.dispatcher.cache.MemoryContextService;
import io.trino.plugin.warp.dispatcher.cache.WarmupElementBlocks;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingCandidate;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.write.StorageWriterSplitConfig;
import io.trino.plugin.warp.storage.write.WarmResult;
import io.trino.plugin.warp.storage.write.WarmupCacheData;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE_FD;
import static io.trino.plugin.warp.storage.flows.FlowsSequencer.INVALID_FLOW_ID;
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
    private final Map<CacheWarmState, CacheAction> cacheActions;
    private final MemoryContextService memoryContextService;
    private final WarmupCacheData warmupCacheData;
    private final RowGroupKey rowGroupKey;
    private final UUID id;
    boolean warmStarted;
    private StorageWriterSplitConfig storageWriterSplitConfig;
    private int totalRecords;

    private boolean warpAbort;
    private long flowId;
    private LocalMemoryContext localMemoryContext;
    private final BlockingDeque<Integer> blocksToProcess;
    private boolean engineAbort;
    private boolean taskStarted;
    private boolean finished;
    private boolean revoked;

    public WarpCacheTask(
            Map<CacheWarmState, CacheAction> cacheActions,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService,
            WarmupCacheData warmupCacheData,
            CacheWarmer cacheWarmer,
            WarmingServiceStats statsWarmingService,
            RowGroupKey rowGroupKey,
            MemoryContextService memoryContextService,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.warmupCacheData = warmupCacheData;
        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.statsWarmingService = requireNonNull(statsWarmingService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.cacheActions = requireNonNull(cacheActions);
        this.memoryContextService = requireNonNull(memoryContextService);
        this.id = UUID.randomUUID();
        this.blocksToProcess = new LinkedBlockingDeque<>();
        this.flowId = INVALID_FLOW_ID;
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
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
            // all resources already released by @revoke
            return;
        }
        if (isAborted()) {
            workerTaskExecutorService.taskFinished(rowGroupKey);
            memoryContextService.remove(this);
            return;
        }
        try {
            loadFromWarmingThread = storageWarmerService.isLoaderAvailable();
            if (!loadFromWarmingThread) {
                storageWarmerService.waitForLoaders();
            }
            statsWarmingService.incwarm_started();
            synchronized (this) {
                if (isAborted()) {
                    return;
                }
                if (initWarmUpProcess()) {
                    Optional<StorageWriterSplitConfig> storageWriterSplitConfigOptional = cacheWarmer.startWarming(rowGroupKey);
                    if (storageWriterSplitConfigOptional.isEmpty()) {
                        shapingLogger.error("Could not start warming. blocksToProcess=%s, warmupCacheData=%s", blocksToProcess, warmupCacheData);
                        return; // cacheWarmState remains ABORT_ON_INIT_PROCESS
                    }
                    storageWriterSplitConfig = storageWriterSplitConfigOptional.get();
                    warmStarted = true;
                    cacheWarmState = initCandidates() ? CacheWarmState.RUNNING : CacheWarmState.ABORTING;
                }
            }
            if (engineAbort) {
                cacheWarmState = CacheWarmState.ABORT_FROM_ENGINE;
                return;
            }
            if (cacheWarmState == CacheWarmState.RUNNING) {
                cacheWarmState = processAll(loadFromWarmingThread);
            }
        }
        finally {
            closeAndSave(cacheWarmState);
            try {
                cacheWarmer.finishWarming(storageWriterSplitConfig);
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed on finish cache warming %s. key=%s", storageWriterSplitConfig, rowGroupKey);
            }
            memoryContextService.releaseMemory(localMemoryContext);
            if (loadFromWarmingThread) {
                storageWarmerService.releaseLoaderThread(true);
            }
            workerTaskExecutorService.taskFinished(rowGroupKey);
            memoryContextService.remove(this);
            statsWarmingService.incwarm_finished();
        }
    }

    private CacheWarmState processAll(boolean loadFromWarmingThread)
    {
        CacheWarmState cacheWarmState = CacheWarmState.RUNNING;

        try {
            while (!isAborted()) {
                if (!loadFromWarmingThread) {
                    storageWarmerService.waitForLoaders();
                }
                int connectorBlockIndex = blocksToProcess.take();
                if (isAborted() || connectorBlockIndex == STOP_TRIGGER) {
                    break;
                }
                List<CacheWarmupElementArgs> cacheWarmupElementsArgsList = warmupCacheData.getCacheWarmupElementArgsList(connectorBlockIndex);
                for (CacheWarmupElementArgs cacheWarmupElementArgs : cacheWarmupElementsArgsList) {
                    if (cacheWarmupElementArgs.isEmpty()) { // in case it was added to the queue and then finished() was called and caused it to be added again
                        continue;
                    }
                    if (finished || cacheWarmupElementArgs.isReady()) {
                        processBlock(cacheWarmupElementArgs);
                    }
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
            if (engineAbort) {
                cacheWarmState = CacheWarmState.ABORT_FROM_ENGINE;
            }
            else if (warpAbort) {
                cacheWarmState = CacheWarmState.ABORTING;
            }
            else {
                cacheWarmState = CacheWarmState.FINISHING;
            }
        }
        return cacheWarmState;
    }

    private void processBlock(CacheWarmupElementArgs cacheWarmupElementArgs)
    {
        WarmingCandidate warmupCandidate = cacheWarmupElementArgs.getWarmupCandidate();
        WarmupElementBlocks warmupElementBlocks = cacheWarmupElementArgs.getWarmupElementBlocks();
        int connectorBlockIndex = cacheWarmupElementArgs.getConnectorBlockIndex();
        WarmResult result = warmupCandidate.pageSink().appendWarmupElementBlocks(warmupElementBlocks);
        if (result.success()) {
            warmupElementBlocks.dropProcessed(result.columnBlockIndex(), result.offset());
            if (warmupElementBlocks.isReady()) {
                // there's still work to do (add first for the case STOP_TRIGGER was already added)
                blocksToProcess.addFirst(connectorBlockIndex);
            }
        }
        else {
            cacheWarmupElementArgs.getWarmupCandidate().setFailedCandidate(); // mark the candidate who caused the failure.
            setWarpAbort();
        }
    }

    public synchronized void warmAsEmptyPageSource()
    {
        if (revoked) {
            logger.debug("task revoked, can't warm as empty page source because of revoke");
            return;
        }
        statsWarmingService.incwarm_started();
        warmupCacheData.getCacheWarmupElementArgsList().forEach(cacheWarmupElementArgs -> {
            WarmingCandidate warmingCandidate = new WarmingCandidate(new long[] {
                    INVALID_FILE_COOKIE_FD,
                    0, 0,
            }, null, 0, cacheWarmupElementArgs.getWarmupElementWriteMetadata(), null, null);
            cacheWarmupElementArgs.setWarmingCandidate(warmingCandidate);
        });
        CacheWarmState cacheWarmState = CacheWarmState.EMPTY_PAGE;
        closeAndSave(cacheWarmState);
        memoryContextService.remove(this);
    }

    private void closeAndSave(CacheWarmState cacheWarmState)
    {
        CacheAction cacheAction = cacheActions.get(cacheWarmState);
        List<WarmingCandidate> warmingCandidates = warmupCacheData.getWarmingCandidates();
        try {
            cacheWarmState = cacheAction.act(warmingCandidates, totalRecords, rowGroupKey);
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to finish handle cache key=%s", rowGroupKey);
            if (cacheWarmState != CacheWarmState.EMPTY_PAGE) {
                cacheWarmState = CacheWarmState.ABORTING;  // so we'll clean the storage
            }
        }
        finally {
            try {
                cacheAction = cacheActions.get(cacheWarmState);
                boolean warmSucceeded = cacheAction.close(
                        warmingCandidates,
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

    private boolean initWarmUpProcess()
    {
        statsWarmingService.incwarm_warp_cache_started();
        try {
            flowId = storageWarmerService.tryRunningWarmFlow(rowGroupKey);
            return true;
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to init warm up process. key=%s", rowGroupKey);
        }
        return false;
    }

    private boolean initCandidates()
    {
        boolean success = true;
        for (CacheWarmupElementArgs cacheColumnArgs : warmupCacheData.getCacheWarmupElementArgsList()) {
            WarmupElementWriteMetadata warmupElementWriteMetadata = cacheColumnArgs.getWarmupElementWriteMetadata();
            RowGroupKey tmpRowGroupKey = cacheWarmer.getTempRowGroupKey(warmupElementWriteMetadata, rowGroupKey);
            try {
                WarmingCandidate warmingCandidate = cacheWarmer.initCandidate(storageWriterSplitConfig, warmupElementWriteMetadata, tmpRowGroupKey);
                cacheColumnArgs.setWarmingCandidate(warmingCandidate);
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to init warm candidate key=%s. %s", tmpRowGroupKey, cacheColumnArgs);
                // failedElement = Optional.of(cacheColumnArgs); // TODO: The one that failed should be marked as temporary failed
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
            shapingLogger.error("Received a page while in finished state. rowGroupKey=%s", rowGroupKey);
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

    /**
     * in case failed to schedule WarpCacheTask we need to clear locks and memory.
     */
    public synchronized void clean()
    {
        if (!isAborted()) {
            blocksToProcess.clear();
            warmupCacheData.clear();
            memoryContextService.remove(this);
        }
    }

    public boolean isRevoked()
    {
        return revoked;
    }

    public synchronized void revoke()
    {
        if (revoked) {
            return;
        }
        revoked = true;
        if (warmStarted) {
            // let flow to clean the data
            setEngineAbort();
            return;
        }
        engineAbort = true;
        blocksToProcess.clear();
        blocksToProcess.add(STOP_TRIGGER);
        warmupCacheData.clear();
        memoryContextService.releaseMemory(localMemoryContext);
        localMemoryContext = null; // we set to null in case it started, so we won't release twice
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
            blocksToProcess.addAll(warmupCacheData.getConnectorColumnIndexes());
            blocksToProcess.add(STOP_TRIGGER);
        }
        else {
            logger.debug("too much memory allocated in cache. currentTaskSize=%s, runningTasksSize=%s probably due to revoke ", warmupCacheData.getRetainedSizeInBytes(), memoryContextService.getRunningSize());
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
            return;
        }
        if (totalRecords == 0) {
            // means query aborted before task added to the queue. just clean the task from the list
            memoryContextService.remove(this);
        }
        this.engineAbort = true;
        blocksToProcess.add(STOP_TRIGGER);
    }

    private boolean isAborted()
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
        WarpCacheTask warpCacheTask = (WarpCacheTask) o;
        return Objects.equals(rowGroupKey, warpCacheTask.rowGroupKey);
    }

    @Override
    public String toString()
    {
        return "WarpCacheTask{" +
                "rowGroupKey=" + rowGroupKey +
                '}';
    }
}
