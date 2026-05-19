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
package io.trino.plugin.warp.storage.capacity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.PathUtils;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.TrinoException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.warp.gen.errorcodes.ErrorCodes.ENV_EXCEPTION_STORAGE_PERMANENT_ERROR;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerCapacityManager
{
    private static final Logger logger = Logger.get(WorkerCapacityManager.class);

    private final GlobalConfig globalConfig;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final StorageEngineConstants storageEngineConstants;
    private final WarmupDemoterStats statsWarmupDemoter;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final CatalogNameProvider catalogNameProvider;
    private final AtomicBoolean workerInitialized = new AtomicBoolean();
    private final AtomicInteger executingTxCount = new AtomicInteger();

    private File warpDir;
    private long totalCapacity;
    private long reservationUsageForSingleTx;

    @Inject
    WorkerCapacityManager(
            GlobalConfig globalConfig,
            WarmupDemoterConfig warmupDemoterConfig,
            StorageEngineConstants storageEngineConstants,
            NativeStorageStateHandler nativeStorageStateHandler,
            MetricsManager metricsManager,
            CatalogNameProvider catalogNameProvider)
    {
        this.globalConfig = requireNonNull(globalConfig);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        statsWarmupDemoter = metricsManager.registerMetric(WarmupDemoterStats.create());
    }

    public void shutdown()
    {
        FileUtils.deleteQuietly(new File(getLocalStoragePath()));
    }

    public synchronized void initWorker()
    {
        if (!workerInitialized.get()) {
            cleanLocalStorage();
            calculateReservationUsageForSingleTx();
            workerInitialized.set(true);
        }
    }

    private void calculateReservationUsageForSingleTx()
    {
        int pageSize = storageEngineConstants.getPageSize();
        reservationUsageForSingleTx = globalConfig.getReservationUsageForSingleTxInBytes() / pageSize;
    }

    public boolean isWorkerInitialized()
    {
        return workerInitialized.get();
    }

    public Integer getExecutingTxCount()
    {
        return executingTxCount.get();
    }

    public double getFractionCurrentUsageFromTotal()
    {
        double fractionUsedUsageFromTotal = (double) getCurrentUsageGross() / (double) getTotalCapacity();
        if (fractionUsedUsageFromTotal > warmupDemoterConfig.getMaxUsageThresholdPercentage()) {
            logger.debug("get fraction usage=%f, executingTxCount=%d", fractionUsedUsageFromTotal, executingTxCount.get());
        }
        return fractionUsedUsageFromTotal;
    }

    public long getCurrentUsageGross()
    {
        int executingTxCount = this.executingTxCount.get();
        int actualExecutingTxCount = executingTxCount > 0 ? executingTxCount - 1 : 0;
        return getCurrentUsage() + actualExecutingTxCount * reservationUsageForSingleTx;
    }

    public long getCurrentUsage()
    {
        return totalCapacity - warpDir.getFreeSpace();
    }

    public void updateCurrentUsage()
    {
        statsWarmupDemoter.setcurrentUsage(getCurrentUsage());
    }

    public long getTotalCapacity()
    {
        return totalCapacity;
    }

    private boolean createCatalogLocalStore()
    {
        String localStorePath = getLocalStoragePath();

        try {
            File file = new File(localStorePath);
            // verify that directory exist
            if (!file.exists()) {
                if (!file.mkdirs() && !file.exists()) {
                    logger.error("local store directory does not exists %s. setting StorageDisableState to permanently disabled", localStorePath);
                    nativeStorageStateHandler.handleErrorCode(ENV_EXCEPTION_STORAGE_PERMANENT_ERROR);
                    return false;
                }
            }

            // verify that write is enabled
            File tempFile = new File(localStorePath + "/temp.temp");
            try (Writer writer = Files.newBufferedWriter(tempFile.toPath(), StandardCharsets.UTF_8)) {
                writer.write(tempFile.getName());
            }
            FileUtils.deleteQuietly(tempFile);
            return true;
        }
        catch (Exception e) {
            logger.error("cannot write to local store path %s. message %s. setting StorageDisableState to permanently disabled", localStorePath, e.getMessage());
            nativeStorageStateHandler.handleErrorCode(ENV_EXCEPTION_STORAGE_PERMANENT_ERROR);
            return false;
        }
    }

    private void calculateTotalCapacity()
    {
        try {
            warpDir = new File(getLocalStoragePath());
            if (!warpDir.exists()) {
                warpDir = warpDir.getParentFile();
            }
            totalCapacity = warpDir.getTotalSpace(); // As we are the sole users of the mount we can use total space
            statsWarmupDemoter.addtotalUsage(totalCapacity);
            logger.info("totalCapacity %dMB", totalCapacity >> 20);
        }
        catch (Exception e) {
            logger.error(e);
            throw new TrinoException(WarpErrorCode.WARP_CONTROL, "could not open local store directory");
        }
    }

    private void cleanLocalStorage()
    {
        if (!createCatalogLocalStore()) {
            logger.warn("cleanLocalStorage exiting since cannot write to local store");
            calculateTotalCapacity();
            return;
        }

        String localStorePath = getLocalStoragePath();
        File localStore = new File(localStorePath);

        if (isEmptyDir(localStore)) {
            logger.info("cleanLocalStorage exiting since no files found in %s", localStorePath);
            calculateTotalCapacity();
            nativeStorageStateHandler.enableTemporarily();
            nativeStorageStateHandler.enablePermanently();
            return;
        }

        logger.info("cleanLocalStorage launching background clean for path %s", localStorePath);

        Thread cleanLocalStorageThread = new Thread(() -> {
            try {
                logger.info("cleanLocalStorage job starting localStorePath %s", localStorePath);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                try {
                    if (!isEmptyDir(localStore)) {
                        FileUtils.deleteDirectory(localStore);
                    }
                }
                catch (IOException io) {
                    logger.warn(io, "Failed to delete directory %s", localStorePath);
                }

                try {
                    //noinspection ResultOfMethodCallIgnored
                    localStore.mkdirs();
                }
                catch (Throwable e) {
                    logger.warn(e, "Failed to create directories %s", localStorePath);
                }

                if (!isEmptyDir(localStore)) {
                    deleteLocalStorageFiles(localStore);
                }

                stopWatch.stop();
                logger.info("cleanLocalStorage job finished. took %d nano sec", stopWatch.getNanoTime());

                // in case we hit an error, we leave total capacity as zero and storage state as permanently failed
                if (isEmptyDir(localStore)) {
                    nativeStorageStateHandler.enableTemporarily();
                    nativeStorageStateHandler.enablePermanently();
                    logger.info(
                            "cleanLocalStorage job finished successfully for [%s]. took %d nano sec",
                            localStorePath,
                            stopWatch.getNanoTime());
                }
                else {
                    logger.error(
                            "cleanLocalStorage job failed to clean [%s]. took %d nano sec",
                            localStorePath,
                            stopWatch.getNanoTime());
                    nativeStorageStateHandler.handleErrorCode(ENV_EXCEPTION_STORAGE_PERMANENT_ERROR);
                }
            }
            finally {
                calculateTotalCapacity();
            }
        });
        cleanLocalStorageThread.start();
    }

    public void deleteLocalStorageFiles()
    {
        deleteLocalStorageFiles(new File(getLocalStoragePath()));
    }

    private void deleteLocalStorageFiles(File localStore)
    {
        Failsafe.with(RetryPolicy.builder()
                        .withMaxRetries(3)
                        .withDelay(Duration.ofSeconds(1))
                        .abortIf(o -> (boolean) o)
                        .build())
                .get(() -> {
                    try {
                        FileUtils.cleanDirectory(localStore);
                        return true;
                    }
                    catch (FileNotFoundException e) {
                        return isEmptyDir(localStore);
                    }
                    catch (Throwable e) {
                        logger.error(e, "deleteLocalStorageFiles failed to clean localStorePath %s", localStore);
                        return false;
                    }
                });
    }

    private boolean isEmptyDir(File directory)
    {
        String[] list = directory.list();
        return (list == null) || (list.length == 0);
    }

    private String getLocalStoragePath()
    {
        return PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get());
    }

    public synchronized void tryAllocateResourcesForWarmupTask()
    {
        updateCurrentUsage();
    }

    public void decrementActiveWarmingTasks()
    {
        executingTxCount.decrementAndGet();
    }

    public void incrementActiveWarmingTasks()
    {
        executingTxCount.incrementAndGet();
    }
}
