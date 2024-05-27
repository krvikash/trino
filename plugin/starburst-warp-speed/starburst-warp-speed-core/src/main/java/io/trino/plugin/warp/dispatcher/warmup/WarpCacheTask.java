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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.cache.CacheAction;
import io.trino.plugin.warp.dispatcher.cache.ParallelWarmUpLimiter;
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
import io.trino.plugin.warp.warmup.exceptions.MaxRowsException;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE_FD;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FLOW_ID;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_TX_ID;
import static java.util.Objects.requireNonNull;

public class WarpCacheTask
        implements WorkerSubmittableTask
{
    private static final Logger logger = Logger.get(WarpCacheTask.class);
    private static final int STOP_TRIGGER = -1;
    private final ShapingLogger shapingLogger;
    private final CacheWarmer cacheWarmer;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final WarmingServiceStats statsWarmingService;
    private final StorageWarmerService storageWarmerService;
    private final List<WarmupElementWriteMetadata> toWarm;
    private final Map<Integer, WarmupElementBlocks> warmupElementBlocksMap;
    private final ParallelWarmUpLimiter parallelWarmUpLimiter;
    private final boolean allocatedWarmResource;
    private final Map<CacheWarmState, CacheAction> cacheActions;

    private final RowGroupKey rowGroupKey;
    private final UUID id;
    private final BlockingQueue<Integer> blocksToProcess;
    boolean warmStarted;
    private StorageWriterSplitConfig storageWriterSplitConfig;
    private List<WarmingCandidate> warmingCandidates;
    private boolean firstIteration;
    private int totalRecords;

    private boolean aborted;
    private boolean finished;
    private long flowId = -1;
    private int txId;

    public WarpCacheTask(
            GlobalConfig globalConfig,
            Map<CacheWarmState, CacheAction> cacheActions,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService,
            ParallelWarmUpLimiter parallelWarmUpLimiter,
            CacheWarmer cacheWarmer,
            WarmingServiceStats statsWarmingService,
            List<WarmupElementWriteMetadata> toWarm,
            Map<Integer, WarmupElementBlocks> warmupElementBlocksMap,
            RowGroupKey rowGroupKey,
            boolean allocatedWarmResource)
    {
        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.cacheWarmer = requireNonNull(cacheWarmer);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.statsWarmingService = requireNonNull(statsWarmingService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.toWarm = requireNonNull(toWarm);
        this.warmupElementBlocksMap = requireNonNull(warmupElementBlocksMap);
        this.parallelWarmUpLimiter = requireNonNull(parallelWarmUpLimiter);
        this.allocatedWarmResource = allocatedWarmResource;
        this.cacheActions = requireNonNull(cacheActions);
        this.id = UUID.randomUUID();
        this.blocksToProcess = new LinkedBlockingQueue<>();
        this.firstIteration = true;
        this.aborted = false;
        this.txId = INVALID_TX_ID;
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
        try {
            statsWarmingService.incwarm_started();
            CacheWarmState cacheWarmState = CacheWarmState.ABORTING;
            try {
                cacheWarmState = processAll();
            }
            finally {
                if (isEmptyPageSource()) {
                    warmingCandidates = toWarm.stream().map(x -> new WarmingCandidate(new long[] {INVALID_FILE_COOKIE_FD, 0}, null, 0, x, null)).collect(Collectors.toList());
                    cacheWarmState = CacheWarmState.EMPTY_PAGE;
                }
                closeAndSave(cacheWarmState);
            }
        }
        finally {
            if (warmStarted) {
                try {
                    cacheWarmer.finishWarmingAndUnlock(txId, storageWriterSplitConfig, rowGroupKey);
                }
                catch (Exception e) {
                    logger.error(e, "failed on finish cache warming %s. key=%s", storageWriterSplitConfig, rowGroupKey);
                }
            }
            parallelWarmUpLimiter.release(toWarm.size());
            if (allocatedWarmResource) {
                storageWarmerService.releaseLoaderThread(true);
            }
            workerTaskExecutorService.taskFinished(rowGroupKey);
            statsWarmingService.incwarm_finished();
        }
    }

    private CacheWarmState processAll()
    {
        CacheWarmState cacheWarmState = CacheWarmState.RUNNING;

        while (!aborted) {
            try {
                int blockIndexToProcess = blocksToProcess.take();
                if (aborted || blockIndexToProcess == STOP_TRIGGER) {
                    break;
                }

                WarmupElementBlocks warmupElementBlocks = warmupElementBlocksMap.get(blockIndexToProcess);
                if (warmupElementBlocks.isEmpty()) { // in case it was added to the queue and then finished() was called and caused it to be added again
                    continue;
                }

                if (firstIteration) {
                    cacheWarmState = init(cacheWarmState);
                    firstIteration = false;
                    if (aborted) {
                        break;
                    }
                }

                if (finished || warmupElementBlocks.isReady()) {
                    processBlock(blockIndexToProcess);
                }
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to warm cache elements");
                cacheWarmState = CacheWarmState.ABORTING;
                aborted = true;
            }
            finally {
                if (blocksToProcess.isEmpty()) {
                    synchronized (blocksToProcess) {
                        blocksToProcess.notifyAll();
                    }
                }
            }
        }

        if (!blocksToProcess.isEmpty()) {
            synchronized (blocksToProcess) {
                blocksToProcess.clear();
                blocksToProcess.notifyAll();
            }
        }

        if (!aborted && !warmupElementBlocksMap.values().stream().allMatch(WarmupElementBlocks::isEmpty)) {
            // This shouldn't happen. The check is for safety, so we won't get wrong results on query
            shapingLogger.error("There's a bug - Not all blocks were fully written");
            cacheWarmState = CacheWarmState.ABORTING;
            aborted = true;
        }

        if (cacheWarmState == CacheWarmState.RUNNING) {
            cacheWarmState = aborted ? CacheWarmState.ABORTING : CacheWarmState.FINISHING;
        }
        return cacheWarmState;
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
                        storageWriterSplitConfig,
                        txId);
                if (warmSucceeded) {
                    statsWarmingService.incwarm_accomplished();
                    statsWarmingService.incwarm_warp_cache_accomplished();
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

    private CacheWarmState init(CacheWarmState state)
            throws InterruptedException
    {
        CacheWarmState newState = state;

        initWarmUpProcess();
        if (aborted) {
            newState = CacheWarmState.ABORT_ON_INIT_PROCESS;
        }
        else {
            storageWriterSplitConfig = cacheWarmer.lockAndStartWarming(rowGroupKey);
            warmStarted = true;
            initCandidates();
            if (aborted) {
                newState = CacheWarmState.ABORTING;
            }
        }

        return newState;
    }

    private void initWarmUpProcess()
    {
        statsWarmingService.incwarm_warp_cache_started();

        try {
            txId = storageWarmerService.warmupOpen(txId);
            flowId = FlowIdGenerator.generateFlowId();
            storageWarmerService.tryRunningWarmFlow(flowId, rowGroupKey);
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to init warm up process. key=%s", rowGroupKey);
            flowId = INVALID_FLOW_ID;
            aborted = true;
        }
    }

    private void initCandidates()
    {
        warmingCandidates = new ArrayList<>(toWarm.size());
        for (WarmupElementWriteMetadata warmUpElementToWarm : toWarm) {
            RowGroupKey tmpRowGroupKey = cacheWarmer.getTempRowGroupKey(warmUpElementToWarm, rowGroupKey);
            try {
                WarmingCandidate warmingCandidate = cacheWarmer.initCandidate(txId, storageWriterSplitConfig, warmUpElementToWarm, tmpRowGroupKey);
                warmingCandidates.add(warmingCandidate);
            }
            catch (Exception e) {
                shapingLogger.error(e, "failed to init warm candidate key=%s. %s", tmpRowGroupKey, warmUpElementToWarm);
                // failedElement = Optional.of(warmUpElementToWarm); // TODO: The one that failed should be marked as temporary failed
                aborted = true;
                break;
            }
        }
    }

    private void processBlock(int blockIndexToProcess)
            throws InterruptedException
    {
        WarmupElementBlocks warmupElementBlocks = warmupElementBlocksMap.get(blockIndexToProcess);
        WarmingCandidate warmingCandidate = warmingCandidates.get(blockIndexToProcess);

        WarmResult result = warmingCandidate.pageSink().appendWarmupElementBlocks(warmupElementBlocks);
        if (result.success()) {
            warmupElementBlocks.dropProcessed(result.columnBlockIndex(), result.offset());
            if (warmupElementBlocks.isReady()) {
                // there's still work to do
                blocksToProcess.put(blockIndexToProcess);
            }
        }
        else {
            // failedElement = Optional.of(warmupElementBlocks.getMetadata()); // TODO: The one that failed should be marked as temporary failed
            aborted = true;
        }
    }

    public void addPage(Page page)
    {
        if (aborted) {
            logger.debug("add page in failed state, do nothing");
            return;
        }
        if (finished) {
            String error = String.format("Received a page while in finished state. rowGroupKey=%s", rowGroupKey);
            shapingLogger.error(error);
            throw new RuntimeException(error);
        }

        try {
            if ((long) totalRecords + (long) page.getPositionCount() >= Integer.MAX_VALUE) {
                throw new MaxRowsException();
            }
            totalRecords += page.getPositionCount();

            for (int blockIndex = 0; blockIndex < page.getChannelCount(); blockIndex++) {
                Block block = page.getBlock(blockIndex);
                WarmupElementBlocks warmupElementBlocks = warmupElementBlocksMap.get(blockIndex);

                boolean ready = warmupElementBlocks.add(block);
                if (ready) {
                    blocksToProcess.add(blockIndex);
                }
                synchronized (blocksToProcess) {
                    while (!blocksToProcess.isEmpty()) {
                        blocksToProcess.wait(); // to make it effectively synchronous
                    }
                }
            }
        }
        catch (Exception e) {
            logFailure(e, rowGroupKey);

            // Trino should call abort() after catching the exception
            throw new RuntimeException(e);
        }
    }

    // this method will be called when there are no more pages to be appended
    public void finish()
    {
        finished = true;
        if (aborted) {
            logger.info("Got finish while on abort state, do nothing. %s", this);
            return;
        }
        // the order of these commands is important. This will flush the remaining data.
        blocksToProcess.addAll(warmupElementBlocksMap.keySet());
        blocksToProcess.add(STOP_TRIGGER);
    }

    public void abort()
    {
        if (aborted) {
            logger.info("Got abort while already on abort state, do nothing. %s", this);
        }
        aborted = true;
        blocksToProcess.add(STOP_TRIGGER);
    }

    private void logFailure(Exception e, RowGroupKey rowGroupKey)
    {
        if (!(e instanceof TrinoException || e instanceof UnsupportedOperationException)) {
            shapingLogger.error(e, "warm failed %s", rowGroupKey);
        }
    }

    private boolean isEmptyPageSource()
    {
        return warmingCandidates == null && firstIteration;
    }
}
