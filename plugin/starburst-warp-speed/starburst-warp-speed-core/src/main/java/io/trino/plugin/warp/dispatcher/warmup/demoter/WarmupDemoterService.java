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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.config.WarmupDemoterConfig.MAX_SUPPORTED_BATCH_SIZE;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarmupDemoterService
{
    private static final Logger logger = Logger.get(WarmupDemoterService.class);

    private final WorkerCapacityManager workerCapacityManager;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final WarmupDemoterStats globalStatsDemoter;
    private final DemoterSync demoterSync;
    private final CatalogName catalogName;
    private final EventBus eventBus;
    private final WarpDeleteService warpDeleteService;
    private final ShapingLogger shapingLogger;

    private final long demoteKey;

    private final AtomicReference<DemoteContext> demoteContext = new AtomicReference<>();
    private final AtomicDouble highestPriorityDemoted = new AtomicDouble(0D);
    private long lastExecutionTime = -1;
    private List<TupleFilter> tupleFilters;

    @Inject
    public WarmupDemoterService(WorkerCapacityManager workerCapacityManager,
            WarmupDemoterConfig warmupDemoterConfig,
            MetricsManager metricsManager,
            DemoterSync demoterSync,
            CatalogName catalogName,
            EventBus eventBus,
            WarpDeleteService warpDeleteService,
            NodeManager nodeManager,
            FlowsSequencer flowsSequencer,
            GlobalConfig globalConfig)
    {
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.globalStatsDemoter = metricsManager.registerMetric(WarmupDemoterStats.create());
        this.demoterSync = requireNonNull(demoterSync);
        this.catalogName = requireNonNull(catalogName);
        this.eventBus = requireNonNull(eventBus);
        this.warpDeleteService = requireNonNull(warpDeleteService);

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());

        demoteKey = demoterSync.registerCatalog(
                this,
                flowsSequencer,
                catalogName,
                nodeManager);
    }

    @PreDestroy
    public void shutdown()
    {
        demoterSync.unregisterCatalog(demoteKey);
    }

    //called from warmup - async
    public boolean tryDemoteStart()
    {
        return tryDemoteStart(null);
    }

    public boolean tryDemoteStart(List<TupleFilter> tupleFilters)
    {
        if (!warmupDemoterConfig.isEnableDemote()) {
            return false;
        }

        if (this.tupleFilters == null) {
            this.tupleFilters = tupleFilters;
        }
        else if (tupleFilters != null) {
            this.tupleFilters = Streams.concat(this.tupleFilters.stream(), tupleFilters.stream())
                    .collect(Collectors.toList());
        }

        boolean isStarted = false;
        if (isExecuting()) {
            logger.debug("%s: is already executing (demoteContext = %s)", catalogName, demoteContext);
            globalStatsDemoter.incnot_executed_due_is_already_executing();
        }
        else {
            try {
                validateInput();
                isStarted = initiateDemoteProcess();
            }
            catch (Exception e) {
                shapingLogger.error(e, "%s: initiateDemoteProcess failed", catalogName);
                globalStatsDemoter.incnumber_of_runs_fail();
            }
        }
        return isStarted;
    }

    boolean initiateDemoteProcess()
    {
        globalStatsDemoter.incnumber_of_calls();

        if (CollectionUtils.isEmpty(tupleFilters)
                && !warmupDemoterConfig.isForceDeleteDeadObjects()
                && !warmupDemoterConfig.isForceDeleteFailedObjects()
                && !aboveThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage())) {
            logger.debug("%s: not executing due thresholds", catalogName);
            globalStatsDemoter.incnot_executed_due_threshold();
            return false;
        }

        logger.debug("%s: call start demote", catalogName);

        if (!demoterSync.tryStartDemoteProcess(demoteKey,
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                Math.min(MAX_SUPPORTED_BATCH_SIZE, warmupDemoterConfig.getBatchSize()),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups())) {
            logger.debug("%s: active demoter was initiated by another connector", catalogName);
            globalStatsDemoter.incnot_executed_due_sync_demote_start_rejected();
            return false;
        }
        logger.debug("%s: call syncDemoteStart startDemote", catalogName);
        return true;
    }

    void connectorSyncStartDemote(
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups)
    {
        logger.debug("catalog[%s]: startDemote", catalogName);
        try {
            workerCapacityManager.updateCurrentUsage();

            initDemoteContext(
                    maxUsageThresholdPercentage,
                    cleanupUsageThresholdPercentage,
                    batchSize,
                    maxElementsToDemoteInIteration,
                    epsilon,
                    isDeleteEmptyRowGroups);

            demoteContext.get().getStatsWarmupDemoter().incnumber_of_runs();

            executeDemote();

            resetHighestPriority();

            logger.debug("%s: call demoteCycleEnd", catalogName);

            demoteCycleEnd();
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                shapingLogger.error("%s: startDemoteCycle failed with error: %s", catalogName, ((TrinoException) e).getErrorCode().getName());
            }
            else {
                shapingLogger.error(e, "%s: startDemoteCycle failed", catalogName);
            }
            cancelDemoteExecution();
        }
    }

    void cancelDemoteExecution()
    {
        shapingLogger.error("catalog[%s]: failed to execute demote, cancelling", catalogName);
        demoterSync.finishDemoteProcess(
                demoteKey,
                demoteContext.get().getLowestPriority(),
                highestPriorityDemoted.get(),
                DemoteStatus.REACHED_THRESHOLD);
        globalStatsDemoter.incnumber_of_runs_fail();
    }

    void connectorSyncDemoteEnd(double highestPriorityDemoted, boolean isFireEvent)
    {
        logger.debug("catalog[%s]: connectorSyncDemoteEnd(highestPriorityDemoted = %f)",
                catalogName, highestPriorityDemoted);
        this.highestPriorityDemoted.set(warmupDemoterConfig.isResetHighestPriority() ? 0 : highestPriorityDemoted);

        if (isExecuting()) {
            globalStatsDemoter.mergeStats(demoteContext.get().getStatsWarmupDemoter());

            if (isFireEvent) {
                fireEventDemoteEnd();
            }
            else {
                logger.debug("catalog[%s]: skip firing event since demote was initiated by another process",
                        catalogName);
            }

            warmupDemoterConfig.setDeleteEmptyRowGroups(false);
            demoteContext.set(null);
        }
    }

    private void fireEventDemoteEnd()
    {
        if (isExecuting()) {
            logger.debug("catalog[%s]: fire event demote end success[%s]", catalogName, true);

            WarmupDemoterFinishEvent event = new WarmupDemoterFinishEvent(
                    true,
                    demoteContext.get().getStatsWarmupDemoter().statsCounterMapper());
            logger.debug("catalog[%s]: fire event demote %s", catalogName, event);
            eventBus.post(event);
        }
    }

    private void executeDemote()
            throws ExecutionException, InterruptedException
    {
        if (isExecuting()) {
            logger.debug("catalog[%s]: execute demote", catalogName);

            if (warmupDemoterConfig.isForceDeleteFailedObjects()) {
                // In case of forceDeleteFailedObjects = false, failed objects will be deleted regularly (as dead objects \ low priority)
                logger.debug("catalog[%s]: deleteFailedObjects = %d",
                        catalogName,
                        demoteContext.get().getTupleRankResult().failedObjects().size());
                deleteFailedObjects(demoteContext.get().getTupleRankResult().failedObjects());
            }
            logger.debug("catalog[%s]: deleteImmediateObjects = %d",
                    catalogName,
                    demoteContext.get().getTupleRankResult().immediateObjects().size());

            deleteImmediateObjects(demoteContext.get().getTupleRankResult().immediateObjects());

            tupleFilters = null;
        }
    }

    @VisibleForTesting
    void initDemoteContext(
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups)
    {
        lastExecutionTime = System.currentTimeMillis();
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(tupleFilters, warmupDemoterConfig.isForceDeleteFailedObjects());
        demoteContext.set(new DemoteContext(
                maxUsageThresholdPercentage,
                cleanupUsageThresholdPercentage,
                batchSize,
                maxElementsToDemoteInIteration,
                epsilon,
                isDeleteEmptyRowGroups,
                tupleRankResult));
        logger.debug("catalog[%s]: initDemoteContext: %s", catalogName, demoteContext);
    }

    private void demoteCycleEnd()
    {
        logger.debug("demoteCycleEnd::start catalog[%s]", catalogName);

        DemoteStatus demoteStatus = getDemoteStatus(
                !aboveThreshold(demoteContext.get().getCleanupUsageThresholdPercentage()),
                true);

        logger.debug("demoteCycleEnd::finish catalog[%s]: demoteCycleEnd: call demoterSync.syncDemoteCycleEnd (lowestPriorityExist = %f, highestPriorityDemoted = %f, demoteStatus = %s)",
                catalogName, demoteContext.get().getLowestPriority(), highestPriorityDemoted.get(), demoteStatus.name());

        demoterSync.finishDemoteProcess(
                demoteKey,
                demoteContext.get().getLowestPriority(),
                highestPriorityDemoted.get(),
                demoteStatus);
    }

    void connectorSyncStartDemoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("catalog[%s]: startDemoteCycle (maxPriorityToDemote = %f, isSingleConnector = %b) tupleRankList.size %d",
                catalogName, maxPriorityToDemote, isSingleConnector, demoteContext.get().getTupleRankList().size());
        try {
            maxPriorityToDemote = isSingleConnector ? Integer.MAX_VALUE : maxPriorityToDemote;

            boolean belowThreshold = demoteCycle(maxPriorityToDemote, isSingleConnector);
            DemoteStatus demoteStatus = getDemoteStatus(belowThreshold, false);
            double lowestPriority = demoteContext.get().getLowestPriority();
            demoterSync.finishDemoteProcess(demoteKey, lowestPriority, highestPriorityDemoted.get(), demoteStatus);
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                shapingLogger.error(e, "catalog[%s]: startDemoteCycle failed with error: %s", catalogName, ((TrinoException) e).getErrorCode().getName());
            }
            else {
                shapingLogger.error(e, "catalog[%s]: startDemoteCycle failed with error", catalogName);
            }
            cancelDemoteExecution();
        }
    }

    private boolean demoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
            throws ExecutionException, InterruptedException
    {
        long elementsDeleted = 0;
        boolean aboveThreshold = true;
        logger.debug("catalog[%s]: demoteCycle: start demoteCycle", catalogName);
        while (aboveThreshold && shouldContinueDemote(isSingleConnector, elementsDeleted, maxPriorityToDemote)) {
            aboveThreshold = aboveThreshold(demoteContext.get().getCleanupUsageThresholdPercentage());
            if (aboveThreshold) {
                long maxElementsToDemote = isSingleConnector ? Integer.MAX_VALUE : demoteContext.get().getMaxElementsToDemote();
                long numberOfElementsToDemote = Math.min(maxElementsToDemote - elementsDeleted, demoteContext.get().getBatchSize());
                long newElementsDeleted = deleteByTupleRank(numberOfElementsToDemote, maxPriorityToDemote);
                elementsDeleted += newElementsDeleted;
            }
            else {
                logger.debug("catalog[%s]: demoteCycle: while loop: reached below threshold", catalogName);
            }
        }
        logger.debug("catalog[%s]: demoteCycle: tupleRankListSize = %d, elementsDeleted = %d, maxElementsToDemote = %d, maxPriorityToDemote = %f, lowestPriorityLeft = %f",
                catalogName, demoteContext.get().getTupleRankList().size(), elementsDeleted, demoteContext.get().getMaxElementsToDemote(),
                maxPriorityToDemote, demoteContext.get().getLowestPriority());

        return !aboveThreshold;
    }

    private boolean shouldContinueDemote(boolean isSingleConnector, long elementsDeleted, double maxPriorityToDemote)
    {
        if (demoteContext.get().getTupleRankList().isEmpty()) {
            return false;
        }

        if (isSingleConnector) {
            return true;
        }
        return elementsDeleted < demoteContext.get().getMaxElementsToDemote() &&
                demoteContext.get().getTupleRankList().getFirst().warmupProperties().priority() < maxPriorityToDemote;
    }

    private void validateInput()
    {
        if (warmupDemoterConfig.getBatchSize() < 1) {
            throw new IllegalArgumentException("batchSize must be greater than 0 - " + warmupDemoterConfig.getBatchSize());
        }
        if (warmupDemoterConfig.getEpsilon() <= 0) {
            throw new IllegalArgumentException("epsilon must be greater than 0 - " + warmupDemoterConfig.getEpsilon());
        }
        if (warmupDemoterConfig.getMaxElementsToDemoteInIteration() < 1) {
            throw new IllegalArgumentException("maxElementsToDemote must be greater than 0 - " + warmupDemoterConfig.getMaxElementsToDemoteInIteration());
        }
    }

    private boolean aboveThreshold(double usageThresholdPercentage)
    {
        return workerCapacityManager.getFractionCurrentUsageFromTotal() > usageThresholdPercentage / 100;
    }

    public boolean canAllowWarmup(double priority)
    {
        return priority >= (highestPriorityDemoted.get() - warmupDemoterConfig.getWarmingPriorityAllowThreshold());
    }

    boolean canAllowWarmup()
    {
        return !aboveThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage());
    }

    // returns false if resource allocation failed (threshold was reached)
    public synchronized boolean tryAllocateNativeResourceForWarmup()
    {
        workerCapacityManager.updateCurrentUsage();
        if (!canAllowWarmup()) {
            workerCapacityManager.decrementActiveWarmingTasks();
            if (workerCapacityManager.getExecutingTxCount() <= 0) {
                tryDemoteStart();
            }
            return false;
        }
        return true;
    }

    @VisibleForTesting
    void deleteImmediateObjects(List<TupleRank> tupleRanks)
            throws ExecutionException, InterruptedException
    {
        logger.debug("catalog[%s]: start deleting %d immediate objects",
                catalogName, tupleRanks.size());
        long deletedObjectsCount = warpDeleteService.delete(
                tupleRanks,
                demoteContext.get(),
                warmupDemoterConfig.isDeleteEmptyRowGroups());
        this.demoteContext.get().getStatsWarmupDemoter().adddead_objects_deleted(deletedObjectsCount);
    }

    @VisibleForTesting
    void deleteFailedObjects(List<TupleRank> failedObjects)
            throws ExecutionException, InterruptedException
    {
        logger.debug("catalog[%s]: start deleting %d failed objects", catalogName, failedObjects.size());
        long deletedObjectsCount = warpDeleteService.delete(
                failedObjects,
                demoteContext.get(),
                warmupDemoterConfig.isDeleteEmptyRowGroups());
        WarmupDemoterStats statsWarmupDemoter = demoteContext.get().getStatsWarmupDemoter();
        statsWarmupDemoter.addfailed_objects_deleted(deletedObjectsCount);
    }

    @VisibleForTesting
    long deleteByTupleRank(long maxElementsToDemote, double maxPriorityToDemote)
            throws ExecutionException, InterruptedException
    {
        if (demoteContext.get().getTupleRankList().isEmpty()) {
            return 0;
        }
        int toIndex = 0;
        List<TupleRank> elementsToDemote = new ArrayList<>();
        while (demoteContext.get().getTupleRankList().size() > toIndex && toIndex < maxElementsToDemote &&
                demoteContext.get().getTupleRankList().get(toIndex).warmupProperties().priority() < maxPriorityToDemote) {
            elementsToDemote.add(demoteContext.get().getTupleRankList().get(toIndex));
            toIndex++;
        }
        long deletedObjectsCount = warpDeleteService.delete(
                elementsToDemote,
                demoteContext.get(),
                warmupDemoterConfig.isDeleteEmptyRowGroups());
        demoteContext.get().getTupleRankList().removeAll(elementsToDemote);
        demoteContext.get().getStatsWarmupDemoter().adddeleted_by_low_priority(deletedObjectsCount);
        double highestPriorityDeleted = elementsToDemote.getLast().warmupProperties().priority();

        logger.debug("catalog[%s]: deleteByTupleRank -> deleted %d elements, highestPriorityDeleted=%s, maxPriorityToDemote=%s",
                catalogName, elementsToDemote.size(), highestPriorityDeleted, maxPriorityToDemote);

        highestPriorityDemoted.set(highestPriorityDeleted);
        return deletedObjectsCount;
    }

    public AtomicDouble getDemoterHighestPriority()
    {
        return highestPriorityDemoted;
    }

    @VisibleForTesting
    public void resetHighestPriority()
    {
        highestPriorityDemoted.set(0);
    }

    private DemoteStatus getDemoteStatus(boolean belowThreshold, boolean resetHighestPriority)
    {
        DemoteStatus demoteStatus = DemoteStatus.NOT_COMPLETED;
        if (demoteContext.get().getTupleRankList().isEmpty()) {
            demoteStatus = DemoteStatus.NO_ELEMENTS_TO_DEMOTE;

            if (resetHighestPriority || CollectionUtils.isEmpty(tupleFilters)) {
                resetHighestPriority();
            }
        }
        else if (belowThreshold) {
            demoteStatus = DemoteStatus.REACHED_THRESHOLD;
        }
        return demoteStatus;
    }

    public boolean isExecuting()
    {
        return demoteContext.get() != null;
    }

    public long getLastExecutionTime()
    {
        return lastExecutionTime;
    }

    public WarmupDemoterStats getCurrentRunStats()
    {
        return !isExecuting() ? null : demoteContext.get().getStatsWarmupDemoter();
    }

    public void setTupleFilters(List<TupleFilter> tupleFilters)
    {
        this.tupleFilters = tupleFilters;
    }

    public double getEpsilon()
    {
        return warmupDemoterConfig.getEpsilon();
    }
}
