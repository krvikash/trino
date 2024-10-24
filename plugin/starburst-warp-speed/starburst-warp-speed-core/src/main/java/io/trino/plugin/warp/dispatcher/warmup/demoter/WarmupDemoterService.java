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
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Singleton
public class WarmupDemoterService
{
    public static final String WARMUP_DEMOTER_STAT_GROUP = "warmupDemoter";
    public static final int MAX_SUPPORTED_BATCH_SIZE = 100;
    private static final Logger logger = Logger.get(WarmupDemoterService.class);

    private final WorkerCapacityManager workerCapacityManager;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final WarmupDemoterStats globalStatsDemoter;
    private final DemoterSync demoterSync;
    private final CatalogName catalogName;
    private final EventBus eventBus;
    private final WarpDeleteService warpDeleteService;

    private final long demoteKey;

    private final AtomicReference<DemoteContext> demoteContext = new AtomicReference<>();
    private final AtomicDouble highestPriorityDemoted = new AtomicDouble(0D);
    private long lastExecutionTime = -1;
    private List<TupleFilter> tupleFilters;
    private boolean forceDeleteDeadObjects;
    private boolean forceDeleteFailedObjects;
    private boolean resetHighestPriority;
    private boolean deleteEmptyRowGroups;
    private boolean enableDemote;

    @Inject
    public WarmupDemoterService(WorkerCapacityManager workerCapacityManager,
            WarmupDemoterConfig warmupDemoterConfig,
            MetricsManager metricsManager,
            DemoterSync demoterSync,
            CatalogName catalogName,
            EventBus eventBus,
            WarpDeleteService warpDeleteService,
            NodeManager nodeManager,
            FlowsSequencer flowsSequencer)
    {
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.globalStatsDemoter = metricsManager.registerMetric(WarmupDemoterStats.create(WARMUP_DEMOTER_STAT_GROUP));
        this.demoterSync = requireNonNull(demoterSync);
        this.catalogName = requireNonNull(catalogName);
        this.eventBus = requireNonNull(eventBus);
        this.warpDeleteService = requireNonNull(warpDeleteService);

        demoteKey = demoterSync.registerCatalog(
                this,
                flowsSequencer,
                catalogName,
                nodeManager);

        enableDemote = warmupDemoterConfig.isEnableDemote();
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
        if (!enableDemote) {
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
        try {
            validateInput();
            isStarted = initiateDemoteProcess();
        }
        catch (Exception e) {
            logger.error(e);
            globalStatsDemoter.incnumber_of_runs_fail();
        }

        if (!isStarted) {
            demoteContext.set(null);
        }
        return isStarted;
    }

    boolean initiateDemoteProcess()
    {
        globalStatsDemoter.incnumber_of_calls();
        if (demoteContext.get() != null) {
            logger.debug("%s: is already executing (demoteContext = %s)", catalogName, demoteContext);
            globalStatsDemoter.incnot_executed_due_is_already_executing();
            return false;
        }

        if (CollectionUtils.isEmpty(tupleFilters)
                && !forceDeleteDeadObjects
                && !forceDeleteFailedObjects
                && !reachedThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage())) {
            logger.debug("%s: not executing due thresholds", catalogName);
            globalStatsDemoter.incnot_executed_due_threshold();
            return false;
        }

        logger.debug("%s: call start demote", catalogName);

        if (!demoterSync.tryStartDemoteProcess(demoteKey)) {
            logger.warn("%s: active demoter was initiated by another connector", catalogName);
            globalStatsDemoter.incnot_executed_due_sync_demote_start_rejected();
            return false;
        }
        logger.debug("%s: call syncDemoteStart startDemote", catalogName);
        return true;
    }

    void connectorSyncStartDemote()
    {
        logger.debug("%s: startDemote", catalogName);
        try {
            if (demoteContext.get() != null) {
                logger.debug("%s: abortActiveDemote", catalogName);
                abortActiveDemote();
            }
            workerCapacityManager.updateCurrentUsage();

            initDemoteContext();

            demoteContext.get().getStatsWarmupDemoter().incnumber_of_runs();

            executeDemote();
            resetHighestPriority();
            logger.debug("%s: call demoteCycleEnd", catalogName);

            demoteCycleEnd();
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                logger.error("%s: startDemoteCycle failed with error: %s", catalogName, ((TrinoException) e).getErrorCode().getName());
            }
            else {
                logger.error(e);
            }
            cancelDemoteExecution();
        }
    }

    void cancelDemoteExecution()
    {
        logger.error("catalog[%s]: failed to execute demote, call native to cancel demote", catalogName);
        demoterSync.finishDemoteProcess(
                demoteKey,
                demoteContext.get().getLowestPriority(),
                highestPriorityDemoted.get(),
                DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD);
        globalStatsDemoter.incnumber_of_runs_fail();
    }

    void connectorSyncDemoteEnd(double highestPriorityDemoted, boolean isFireEvent)
    {
        logger.debug("catalog[%s]: connectorSyncDemoteEnd(highestPriorityDemoted = %f)",
                catalogName, highestPriorityDemoted);
        this.highestPriorityDemoted.set(resetHighestPriority ? 0 : highestPriorityDemoted);

        if (isFireEvent) {
            fireEventDemoteEnd(true);
        }

        if (demoteContext.get() != null) {
            deleteEmptyRowGroups = false;
            demoteContext.set(null);
            logger.debug("catalog[%s]: connectorSyncDemoteEnd(demoteContext = null)", catalogName);
        }
        else {
            logger.error("catalog[%s]: connectorSyncDemoteEnd was called by another process, or demote was canceled",
                    catalogName);
        }
    }

    private void fireEventDemoteEnd(boolean success)
    {
        logger.debug("catalog[%s]: fire event demote end success[%s]", catalogName, success);

        if (demoteContext.get() != null) {
            globalStatsDemoter.mergeStats(demoteContext.get().getStatsWarmupDemoter());

            WarmupDemoterFinishEvent event = new WarmupDemoterFinishEvent(
                    success,
                    demoteContext.get().getStatsWarmupDemoter().statsCounterMapper());
            logger.debug("catalog[%s]: fire event demote %s", catalogName, event);
            eventBus.post(event);
        }
    }

    private void executeDemote()
            throws ExecutionException, InterruptedException
    {
        logger.debug("catalog[%s]: execute demote", catalogName);

        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(tupleFilters, forceDeleteFailedObjects);
        demoteContext.get().setTupleRankList(tupleRankResult.tupleRankList());
        if (forceDeleteFailedObjects) {
            // In case of forceDeleteFailedObjects = false, failed objects will be deleted regulary (as dead objects \ low priority)
            logger.debug("catalog[%s]: deleteFailedObjects = %d", catalogName, tupleRankResult.failedObjects().size());
            deleteFailedObjects(tupleRankResult.failedObjects());
        }
        logger.debug("catalog[%s]: deleteImmediateObjects = %d", catalogName, tupleRankResult.immediateObjects().size());
        deleteImmediateObjects(tupleRankResult.immediateObjects());
        sortTupleRankCollection(demoteContext.get().getTupleRankList());
        tupleFilters = null;
    }

    @VisibleForTesting
    void initDemoteContext()
    {
        lastExecutionTime = System.currentTimeMillis();
        int batchSize = Math.min(MAX_SUPPORTED_BATCH_SIZE, warmupDemoterConfig.getBatchSize());
        demoteContext.set(new DemoteContext(
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                batchSize,
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                deleteEmptyRowGroups));
        logger.debug("catalog[%s]: initDemoteContext: %s", catalogName, demoteContext);
    }

    private void demoteCycleEnd()
    {
        logger.debug("demoteCycleEnd::start catalog[%s]", catalogName);

        DemoteStatus demoteStatus;
        if (demoteContext.get().getTupleRankList().isEmpty()) {
            resetHighestPriority();
            demoteStatus = DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE;
            if (CollectionUtils.isEmpty(tupleFilters)) {
                resetHighestPriority();
            }
        }
        else if (reachedThreshold(demoteContext.get().getCleanupUsageThresholdPercentage())) {
            demoteStatus = DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
        }
        else {
            demoteStatus = DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD;
        }

        logger.debug("demoteCycleEnd::finish catalog[%s]: demoteCycleEnd: call demoterSync.syncDemoteCycleEnd (lowestPriorityExist = %f, highestPriorityDemoted = %f, demoteStatus = %s)",
                catalogName, demoteContext.get().getLowestPriority(), highestPriorityDemoted.get(), demoteStatus.name());

        demoterSync.finishDemoteProcess(
                demoteKey,
                demoteContext.get().getLowestPriority(),
                highestPriorityDemoted.get(),
                demoteStatus);
    }

    void abortActiveDemote()
    {
        logger.error("catalog[%s]: demote was aborted, demoteContext = %s", catalogName, demoteContext);
        deleteEmptyRowGroups = false;
        globalStatsDemoter.incnumber_of_runs_fail();
        fireEventDemoteEnd(false);
        demoteContext.set(null);
    }

    void connectorSyncStartDemoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("catalog[%s]: startDemoteCycle (maxPriorityToDemote = %f, isSingleConnector = %b) tupleRankList.size %d",
                catalogName, maxPriorityToDemote, isSingleConnector, demoteContext.get().getTupleRankList().size());
        try {
            maxPriorityToDemote = isSingleConnector ? Integer.MAX_VALUE : maxPriorityToDemote;

            if (!hasElementsToDemote(maxPriorityToDemote)) {
                logger.error("should not call demote to on catalog[%s] -> tupleRankList[%s], lowestPriority[%s], maxPriorityToDemote[%s]",
                        catalogName,
                        demoteContext.get().getTupleRankList().isEmpty(),
                        demoteContext.get().getLowestPriority(),
                        maxPriorityToDemote);
            }
            boolean reachedThreshold = demoteCycle(maxPriorityToDemote, isSingleConnector);
            DemoteStatus demoteStatus;
            if (reachedThreshold) {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD;
            }
            else if (demoteContext.get().getTupleRankList().isEmpty()) {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE;
                if (CollectionUtils.isEmpty(tupleFilters)) {
                    resetHighestPriority();
                }
            }
            else {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
            }
            double lowestPriority = demoteContext.get().getLowestPriority();
            demoterSync.finishDemoteProcess(demoteKey, lowestPriority, highestPriorityDemoted.get(), demoteStatus);
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                logger.error(e,
                        "catalog[%s]: startDemoteCycle failed with error: %s",
                        catalogName, ((TrinoException) e).getErrorCode().getName());
            }
            else {
                logger.error(e);
            }
            cancelDemoteExecution();
        }
    }

    private boolean demoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
            throws ExecutionException, InterruptedException
    {
        long elementsDeleted = 0;
        boolean reachedThreshold = false;
        logger.debug("catalog[%s]: demoteCycle: start demoteCycle", catalogName);
        while (shouldContinueDemote(reachedThreshold, isSingleConnector, elementsDeleted, maxPriorityToDemote)) {
            if (reachedThreshold(demoteContext.get().getCleanupUsageThresholdPercentage())) {
                long maxElementsToDemote = isSingleConnector ? Integer.MAX_VALUE : demoteContext.get().getMaxElementsToDemote();
                long numberOfElementsToDemote = Math.min(maxElementsToDemote - elementsDeleted, demoteContext.get().getBatchSize());
                long newElementsDeleted = deleteByTupleRank(numberOfElementsToDemote, maxPriorityToDemote);
                elementsDeleted += newElementsDeleted;
            }
            else {
                reachedThreshold = true;
                logger.warn("catalog[%s]: demoteCycle: while loop: reached threshold", catalogName);
            }
        }
        logger.debug("catalog[%s]: demoteCycle: tupleRankListSize = %d, elementsDeleted = %d, maxElementsToDemote = %d, maxPriorityToDemote = %f, lowestPriorityLeft = %f",
                catalogName, demoteContext.get().getTupleRankList().size(), elementsDeleted, demoteContext.get().getMaxElementsToDemote(),
                maxPriorityToDemote, demoteContext.get().getLowestPriority());

        return reachedThreshold;
    }

    private boolean shouldContinueDemote(boolean reachedThreshold, boolean isSingleConnector, long elementsDeleted, double maxPriorityToDemote)
    {
        if (reachedThreshold || demoteContext.get().getTupleRankList().isEmpty()) {
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

    private boolean hasElementsToDemote(double priority)
    {
        return !demoteContext.get().getTupleRankList().isEmpty() && demoteContext.get().getLowestPriority() < priority;
    }

    private boolean reachedThreshold(double usageThresholdPercentage)
    {
        return workerCapacityManager.getFractionCurrentUsageFromTotal() > usageThresholdPercentage / 100;
    }

    public boolean canAllowWarmup(double priority)
    {
        return priority >= (highestPriorityDemoted.get() - warmupDemoterConfig.getWarmingPriorityAllowThreshold());
    }

    boolean canAllowWarmup()
    {
        return !reachedThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage());
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
        long deletedObjectsCount = warpDeleteService.delete(tupleRanks, demoteContext.get(), deleteEmptyRowGroups);
        this.demoteContext.get().getStatsWarmupDemoter().adddead_objects_deleted(deletedObjectsCount);
    }

    @VisibleForTesting
    void deleteFailedObjects(List<TupleRank> failedObjects)
            throws ExecutionException, InterruptedException
    {
        logger.debug("catalog[%s]: start deleting %d failed objects", catalogName, failedObjects.size());
        long deletedObjectsCount = warpDeleteService.delete(failedObjects, demoteContext.get(), deleteEmptyRowGroups);
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
        logger.debug("catalog[%s]: start deleteByWarmupElement %d", catalogName, elementsToDemote.size());
        long deletedObjectsCount = warpDeleteService.delete(elementsToDemote, demoteContext.get(), deleteEmptyRowGroups);
        demoteContext.get().getTupleRankList().removeAll(elementsToDemote);
        demoteContext.get().getStatsWarmupDemoter().adddeleted_by_low_priority(deletedObjectsCount);
        double highestPriorityDeleted = elementsToDemote.getLast().warmupProperties().priority();

        logger.debug("catalog[%s]: deleteByWarmupElement -> highestPriorityDeleted=%s",
                catalogName, highestPriorityDeleted);

        highestPriorityDemoted.set(highestPriorityDeleted);
        return deletedObjectsCount;
    }

    void sortTupleRankCollection(List<TupleRank> tupleRankList)
    {
        Collections.sort(tupleRankList);
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
        return demoteContext.get() == null ? null : demoteContext.get().getStatsWarmupDemoter();
    }

    public void setTupleFilters(List<TupleFilter> tupleFilters)
    {
        this.tupleFilters = tupleFilters;
    }

    public void setForceDeleteDeadObjects(boolean forceDeleteDeadObjects)
    {
        this.forceDeleteDeadObjects = forceDeleteDeadObjects;
    }

    public void setForceDeleteFailedObjects(boolean forceDeleteFailedObjects)
    {
        this.forceDeleteFailedObjects = forceDeleteFailedObjects;
    }

    public void setResetHighestPriority(boolean resetHighestPriority)
    {
        this.resetHighestPriority = resetHighestPriority;
    }

    public void setDeleteEmptyRowGroups(boolean deleteEmptyRowGroups)
    {
        this.deleteEmptyRowGroups = deleteEmptyRowGroups;
    }

    public void setEnableDemote(boolean enableDemote)
    {
        this.enableDemote = enableDemote;
    }

    public double getEpsilon()
    {
        return warmupDemoterConfig.getEpsilon();
    }
}
