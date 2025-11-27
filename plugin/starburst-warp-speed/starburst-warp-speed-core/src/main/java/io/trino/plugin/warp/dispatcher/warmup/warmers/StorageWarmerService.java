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
package io.trino.plugin.warp.dispatcher.warmup.warmers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.flows.FlowType;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.storage.write.PageSink;
import io.trino.plugin.warp.storage.write.WarmUpState;
import io.trino.plugin.warp.util.StorageUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_NUM_OF;
import static io.trino.plugin.warp.gen.errorcodes.ErrorCodes.ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageWarmerService
{
    private static final Logger logger = Logger.get(StorageWarmerService.class);
    public static final long INVALID_FILE_COOKIE_FD = -1;

    private final RowGroupDataService rowGroupDataService;
    private final StorageEngine storageEngine;
    private final GlobalConfig globalConfig;
    private final WarmupDemoterService warmupDemoterService;
    private final StorageEngineTxService storageEngineTxService;
    private final FlowsSequencer flowsSequencer;
    private final WarmingServiceStats statsWarmingService;
    private final ShapingLogger shapingLogger;
    private final WorkerCapacityManager workerCapacityManager;
    private final NativeStorageStateHandler nativeStorageStateHandler;

    @Inject
    public StorageWarmerService(RowGroupDataService rowGroupDataService,
            StorageEngine storageEngine,
            GlobalConfig globalConfig,
            WarmupDemoterService warmupDemoterService,
            StorageEngineTxService storageEngineTxService,
            FlowsSequencer flowsSequencer,
            MetricsManager metricsManager,
            WorkerCapacityManager workerCapacityManager,
            NativeStorageStateHandler nativeStorageStateHandler,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageEngine = requireNonNull(storageEngine);
        this.globalConfig = requireNonNull(globalConfig);
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.storageEngineTxService = requireNonNull(storageEngineTxService);
        this.flowsSequencer = requireNonNull(flowsSequencer);
        this.statsWarmingService = metricsManager.registerMetric(WarmingServiceStats.create());
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);

        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    public void createFile(RowGroupKey rowGroupKey)
            throws IOException
    {
        String rowGroupFilePath = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        File file = new File(rowGroupFilePath);
        if (!file.exists()) {
            try {
                FileUtils.createParentDirectories(file);
            }
            catch (Throwable e) {
                nativeStorageStateHandler.handleErrorCode(ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR);
                throw e;
            }
        }
    }

    public long[] fileOpen(RowGroupKey rowGroupKey)
    {
        String rowGroupFilePath = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        long[] fileCookie = new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()];
        // fileCookie.fd was initialized to -1. In case fileOpen throws an exception we will not close it in the 'finally' clause
        fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()] = storageEngine.fileOpen(rowGroupFilePath);
        fileCookie[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()] = StorageUtils.fileHash64(rowGroupFilePath);
        fileCookie[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()] = rowGroupKey.fileModifiedTime();

        return fileCookie;
    }

    public void flushRecords(long[] fileCookie, RowGroupData rowGroupData)
    {
        if (fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()] != INVALID_FILE_COOKIE_FD) {
            if (rowGroupData.getValidWarmUpElements().isEmpty()) {
                rowGroupDataService.deleteData(rowGroupData, false);
                logger.debug("all we failed for row group=%s", rowGroupData.getRowGroupKey());
            }
            else {
                rowGroupDataService.flush(rowGroupData.getRowGroupKey());
            }
        }
    }

    public WarmSinkResult sinkClose(PageSink pageSink,
            WarmupElementWriteMetadata currWarmUpElement,
            int rowCount,
            boolean isValidWE,
            int currentOffset,
            long[] fileCookie)
    {
        WarmUpElement updatedWarmupElement;
        int newOffset = currentOffset;
        if (rowCount > 0) {
            // close might fail as well, we need to check the success elements after returning
            WarmSinkResult warmSinkResult = pageSink.close(rowCount);
            updatedWarmupElement = warmSinkResult.warmUpElement();
            isValidWE &= updatedWarmupElement.isValid();
            if (isValidWE) {
                newOffset = warmSinkResult.offset();
            }
            else {
                fileTruncate(fileCookie, currentOffset);
            }
        }
        else {
            //case we never init sink it means that total rowCount is 0, and RowGroup will be mark as EmptyPageSource
            updatedWarmupElement = currWarmUpElement.warmUpElement();
        }
        return new WarmSinkResult(updatedWarmupElement, newOffset);
    }

    public void fileTruncate(long[] fileCookie, int currentOffset)
    {
        storageEngine.fileTruncate((int) fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()], currentOffset);
    }

    public void fileClose(long[] fileCookie, Optional<RowGroupData> rowGroupData)
    {
        if (fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()] != INVALID_FILE_COOKIE_FD) {
            try {
                storageEngine.fileClose((int) fileCookie[FILE_COOKIE_PARAMS_FD.ordinal()]);
            }
            catch (Exception e) {
                if (rowGroupData.isPresent()) {
                    rowGroupDataService.deleteData(rowGroupData.get(), true);
                    String rowGroupFilePath = rowGroupData.get().getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath());
                    shapingLogger.error(e, "failed to close file %s", rowGroupFilePath);
                }
            }
        }
        else if (rowGroupData.isPresent()) { // we failed in opening the file
            rowGroupDataService.deleteData(rowGroupData.get(), true);
            String rowGroupFilePath = rowGroupData.get().getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath());
            shapingLogger.error("failed to open file %s", rowGroupFilePath);
        }
    }

    public void verifyQueryOffsets(RowGroupKey rowGroupKey, List<WarmUpElement> validWarmUpElements, WarmUpState warmUpState)
    {
        long[] fileCookieParams = new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()];
        fileCookieParams[FILE_COOKIE_PARAMS_FD.ordinal()] = INVALID_FILE_COOKIE_FD;
        try {
            if (!validWarmUpElements.isEmpty() && (validWarmUpElements.getFirst().getTotalRecords() < 32 * 1024)) {
                fileCookieParams = fileOpen(rowGroupKey);
                warmUpState.setFileCookie(
                        (int) fileCookieParams[FILE_COOKIE_PARAMS_FD.ordinal()],
                        fileCookieParams[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()],
                        fileCookieParams[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()]);
                for (WarmUpElement warmUpElement : validWarmUpElements) {
                    int queryOffset = warmUpElement.getQueryOffset();
                    if (queryOffset > 0) { // @TODO there are issues with offset zero should be investigated
                        logger.debug("verifying offset %d WE %s", queryOffset, warmUpElement);
                        warmUpState.setStartOffset(queryOffset);
                        storageEngine.warmupVerifyQueryOffset(warmUpState.getMemory());
                    }
                }
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to verify query offsets for %s", validWarmUpElements);
            throw new RuntimeException(e);
        }
        finally {
            fileClose(fileCookieParams, Optional.empty());
        }
    }

    public void releaseRowGroup(RowGroupData rowGroupData, boolean locked)
    {
        if (rowGroupData != null && locked) {
            rowGroupData.getLock().writeUnlock();
        }
    }

    public void lockRowGroup(RowGroupData rowGroupData)
            throws InterruptedException
    {
        requireNonNull(rowGroupData);
        rowGroupData.getLock().writeLock();
    }

    public void releaseLoaderThread(boolean skipWait)
    {
        storageEngineTxService.doneWarming(skipWait);
    }

    public boolean finishWarm(long flowId,
            boolean releaseTx,
            boolean force,
            boolean runDemote)
    {
        releaseTx(releaseTx);

        boolean finish = flowsSequencer.flowFinished(FlowType.WARMUP, flowId, force);

        if (runDemote) {
            try {
                warmupDemoterService.tryDemoteStart();
            }
            catch (Throwable ignored) {
                logger.warn("demoter failed");
            } //do nothing
        }

        return finish;
    }

    public void releaseTx(boolean releaseTx)
    {
        if (releaseTx) {
            workerCapacityManager.decrementActiveWarmingTasks();
        }
    }

    public void waitForLoaders()
    {
        CompletableFuture<Boolean> future = storageEngineTxService.tryToWarm();
        try {
            future.get();
        }
        catch (Exception e) {
            logger.warn(e, "failed to release waiting warm");
        }
    }

    public boolean isLoaderAvailable()
    {
        return storageEngineTxService.isLoaderAvailable();
    }

    public long tryRunningWarmFlow(RowGroupKey rowGroupKey)
            throws ExecutionException, InterruptedException
    {
        return flowsSequencer.tryRunningFlow(
                        FlowType.WARMUP,
                        Optional.of(rowGroupKey.toString()))
                .get();
    }

    public boolean verifyNativeResourceForWarmup()
    {
        return warmupDemoterService.canAllowWarmup();
    }

    public boolean tryAllocateNativeResourceForWarmup()
    {
        boolean acquiredWarmup = warmupDemoterService.tryAllocateNativeResourceForWarmup();
        if (!acquiredWarmup) {
            statsWarmingService.incwarm_skipped_due_reaching_threshold();
        }
        return acquiredWarmup;
    }
}
