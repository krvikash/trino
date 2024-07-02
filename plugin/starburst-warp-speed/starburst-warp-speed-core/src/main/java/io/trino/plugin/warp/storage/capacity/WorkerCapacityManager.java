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
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.PathUtils;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.plugin.warp.util.WarpInitializedServiceMarker;
import io.trino.spi.TrinoException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerCapacityManager
        implements WarpInitializedServiceMarker
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
    WorkerCapacityManager(GlobalConfig globalConfig,
            WarmupDemoterConfig warmupDemoterConfig,
            StorageEngineConstants storageEngineConstants,
            NativeStorageStateHandler nativeStorageStateHandler,
            WarpInitializedServiceRegistry warpInitializedServiceRegistry,
            MetricsManager metricsManager,
            CatalogNameProvider catalogNameProvider)
    {
        this.globalConfig = requireNonNull(globalConfig);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        statsWarmupDemoter = metricsManager.registerMetric(WarmupDemoterStats.create(WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP));
        warpInitializedServiceRegistry.addService(this);
    }

    @Override
    public void init()
    {
        cleanLocalStorage();
    }

    public synchronized void initWorker()
    {
        if (workerInitialized.compareAndSet(false, true)) {
            calculateReservationUsageForSingleTx();
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

    public void setCurrentUsage()
    {
        statsWarmupDemoter.setcurrentUsage(getCurrentUsage());
    }

    public long getTotalCapacity()
    {
        return totalCapacity;
    }

    public void increaseExecutingTx()
    {
        executingTxCount.incrementAndGet();
    }

    public void setExecutingTx(int executingTx)
    {
        executingTxCount.set(executingTx);
        statsWarmupDemoter.setreserved_tx(executingTx);
    }

    public void decreaseExecutingTx()
    {
        executingTxCount.decrementAndGet();
    }

    private boolean createCatalogLocalStore()
    {
        String localStorePath = PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get());

        try {
            // verify that directory exist
            if (!Files.exists(Paths.get(localStorePath))) {
                if (!new File(localStorePath).mkdirs()) {
                    logger.error("local store directory does not exists %s. setting StorageDisableState to permanently disabled", localStorePath);
                    nativeStorageStateHandler.setStorageDisableState(true, false);
                    return false;
                }
            }

            // verify that write is enabled
            File tempFile = new File(localStorePath + "/temp.temp");
            Writer writer = Files.newBufferedWriter(tempFile.toPath(), StandardCharsets.UTF_8);
            writer.write(tempFile.getName());
            writer.close();
            tempFile.delete();
            return true;
        }
        catch (Exception e) {
            logger.error("cannot write to local store path %s. message %s. setting StorageDisableState to permanently disabled", localStorePath, e.getMessage());
            nativeStorageStateHandler.setStorageDisableState(true, false);
            return false;
        }
    }

    private void calculateTotalCapacity()
    {
        String localStorePath = PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get());

        try {
            warpDir = new File(localStorePath);
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
            logger.info("cleanLocalStorage exiting since cannot write to local store");
            calculateTotalCapacity();
            return;
        }

        if (!globalConfig.isEnableLocalStoreCleanOnLoad()) {
            logger.info("cleanLocalStorage exiting since clean is disabled");
            calculateTotalCapacity();
            return;
        }

        String localStorePath = PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get());
        File localStore = new File(localStorePath);
        String[] ls = localStore.list();

        if ((ls == null) || (ls.length == 0)) {
            logger.info("cleanLocalStorage exiting since no files found");
            calculateTotalCapacity();
            return;
        }

        logger.info("cleanLocalStorage launching background clean for path %s", localStorePath);
        nativeStorageStateHandler.setStorageDisableState(true, false);
        Thread cleanLocalStorageThread = new Thread(() -> {
            try {
                logger.info("cleanLocalStorage job starting localStorePath %s", localStorePath);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                String[] fileList = localStore.list();
                boolean cleaned = ((fileList == null) || (fileList.length == 0));
                int iterations = 0;

                while (!cleaned && (iterations < 3)) {
                    try {
                        FileUtils.cleanDirectory(new File(localStorePath));
                        cleaned = true;
                    }
                    catch (FileNotFoundException e) {
                        fileList = localStore.list();
                        cleaned = ((fileList == null) || (fileList.length == 0));
                    }
                    iterations++;
                }

                stopWatch.stop();
                logger.info("cleanLocalStorage job finished. took %d nano sec %d iterations", stopWatch.getNanoTime(), iterations);
                // in case we hit an error, we leave total capacity as zero and storage state as permanently failed
                nativeStorageStateHandler.setStorageDisableState(false, false);
            }
            catch (IOException e) {
                logger.error(e, "cleanLocalStorage job failed to clean localStorePath %s", localStorePath);
            }
            finally {
                calculateTotalCapacity();
            }
        });
        cleanLocalStorageThread.start();
    }
}
