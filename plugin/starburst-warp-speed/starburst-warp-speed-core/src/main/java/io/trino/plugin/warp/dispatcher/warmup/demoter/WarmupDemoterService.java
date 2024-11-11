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
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.flows.FlowIdGenerator;
import io.trino.plugin.warp.storage.flows.FlowType;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.TrinoException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.warmup.WarmupProperties.NA_TTL;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("ALL")
@Singleton
public class WarmupDemoterService
{
    public static final String WARMUP_DEMOTER_STAT_GROUP = "warmupDemoter";
    public static final int MAX_SUPPORTED_BATCH_SIZE = 100;
    public static final int FAILED_DEMOTE_SQUENCE = -1;
    private static final Logger logger = Logger.get(WarmupDemoterService.class);
    private final WorkerCapacityManager workerCapacityManager;
    private final ConnectorSync connectorSync;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final WarmupDemoterStats globalStatsDemoter;
    private final FlowsSequencer flowsSequencer;
    private WarmupProperties defaultWarmupProperties;
    private AtomicDouble highestPriority = new AtomicDouble(0);
    private AtomicBoolean isExecuting = new AtomicBoolean(false);
    private long lastExecutionTime = -1;
    private CatalogNameProvider catalogNameProvider;
    private List<TupleFilter> tupleFilters;
    private boolean forceDeleteDeadObjects;
    private boolean forceDeleteFailedObjects;
    private boolean resetHigestPriority;
    private boolean deleteEmptyRowGroups;
    private boolean enableDemote;
    private EventBus eventBus;
    private DemoteContext demoteContext;
    private WarpDeleteService warpDeleteService;

    @Inject
    public WarmupDemoterService(WorkerCapacityManager workerCapacityManager,
                                WarmupDemoterConfig warmupDemoterConfig,
                                MetricsManager metricsManager,
                                FlowsSequencer flowsSequencer,
                                ConnectorSync connectorSync,
                                CatalogNameProvider catalogNameProvider,
                                EventBus eventBus,
                                WarpDeleteService warpDeleteService)
    {
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.globalStatsDemoter = (WarmupDemoterStats) metricsManager.registerMetric(WarmupDemoterStats.create(WARMUP_DEMOTER_STAT_GROUP));
        this.flowsSequencer = requireNonNull(flowsSequencer);
        this.connectorSync = requireNonNull(connectorSync);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.eventBus = requireNonNull(eventBus);
        this.defaultWarmupProperties = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, warmupDemoterConfig.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE);
        this.enableDemote = warmupDemoterConfig.isEnableDemote();
        this.warpDeleteService = requireNonNull(warpDeleteService);
        init();
    }

    private void init()
    {
        logger.debug("WarmupDemoterService init = %d", System.identityHashCode(connectorSync));
        connectorSync.init(this);
    }

    //called from warmup - async
    public int tryDemoteStart()
    {
        return tryDemoteStart(null);
    }

    public int tryDemoteStart(List<TupleFilter> tupleFilters)
    {
        if (!enableDemote) {
            return FAILED_DEMOTE_SQUENCE;
        }

        if (this.tupleFilters == null) {
            this.tupleFilters = tupleFilters;
        }
        else if (tupleFilters != null) {
            this.tupleFilters = Streams.concat(this.tupleFilters.stream(), tupleFilters.stream())
                    .collect(Collectors.toList());
        }

        int demoteSequence;
        try {
            validateInput();
            demoteSequence = initiateDemoteProcess();
        }
        catch (Exception e) {
            logger.error(e);
            globalStatsDemoter.incnumber_of_runs_fail();
            demoteSequence = FAILED_DEMOTE_SQUENCE;
        }

        return demoteSequence;
    }

    int initiateDemoteProcess()
    {
        globalStatsDemoter.incnumber_of_calls();
        if (isExecuting.get() || demoteContext != null) {
            logger.debug("%s: is already executing (isExecuting = %b, demoteContext = %s)", catalogNameProvider.get(), isExecuting.get(), demoteContext);
            globalStatsDemoter.incnot_executed_due_is_already_executing();
            return FAILED_DEMOTE_SQUENCE;
        }

        if (CollectionUtils.isEmpty(tupleFilters)
                && !forceDeleteDeadObjects
                && !forceDeleteFailedObjects
                && !reachedThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage())) {
            logger.debug("%s: not executing due thresholds", catalogNameProvider.get());
            globalStatsDemoter.incnot_executed_due_threshold();
            return FAILED_DEMOTE_SQUENCE;
        }

        logger.debug("%s: call start demote", catalogNameProvider.get());
        int demoterSequence = connectorSync.syncDemotePrepare(warmupDemoterConfig.getEpsilon());
        if (demoterSequence == FAILED_DEMOTE_SQUENCE) {
            logger.warn("%s: active demoter was initiated by another connector", catalogNameProvider.get());
            globalStatsDemoter.incnot_executed_due_sync_demote_start_rejected();
            isExecuting.set(false);
            return FAILED_DEMOTE_SQUENCE;
        }
        logger.debug("%s: call syncDemoteStart startDemote with seqId = %d", catalogNameProvider.get(), demoterSequence);
        connectorSync.startDemote(demoterSequence);
        return demoterSequence;
    }

    public void connectorSyncStartDemote(int demoterSequence)
    {
        logger.debug("%s: startDemote(demoterSequence = %d)", catalogNameProvider.get(), demoterSequence);
        try {
            if (demoteContext != null && demoteContext.getDemoterSequence() != demoterSequence) {
                logger.debug("%s: abortActiveDemote(demoteContext.demoterSequence = %d)", catalogNameProvider.get(), demoteContext.getDemoterSequence());
                abortActiveDemote();
            }
            workerCapacityManager.setCurrentUsage();

            initDemoteContext(demoterSequence);

            executeDemote(demoterSequence);
            highestPriority.set(0);
            logger.debug("%s: call demoteCycleEnd, demoterSequence = %d", catalogNameProvider.get(), demoteContext.getDemoterSequence());

            demoteCycleEnd();
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                logger.error("%s: startDemoteCycle failed with error: %s", catalogNameProvider.get(), ((TrinoException) e).getErrorCode().getName());
            }
            else {
                logger.error(e);
            }
            cancelDemoteExecution();
        }
    }

    void cancelDemoteExecution()
    {
        logger.error("failed to execute demote, call native to cancel demote with sequenceId = %d", demoteContext.getDemoterSequence());
        connectorSync.syncDemoteEnd(demoteContext.getDemoterSequence(),
                                    demoteContext.getLowestPriority(),
                                    highestPriority.get(),
                                    DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD);
        globalStatsDemoter.incnumber_of_runs_fail();
    }

    public void connectorSyncDemoteEnd(int demoteSequence, double highestPriority)
    {
        logger.debug("%s: demoteEnd(demoteSequence = %d, highestPriority = %f)", catalogNameProvider.get(), demoteSequence, highestPriority);
        this.highestPriority.set(resetHigestPriority ? 0 : highestPriority);
        if (isExecuting()) {
            globalStatsDemoter.incnumber_of_runs();
            demoteContext.getStopWatch().stop();
            deleteEmptyRowGroups = false;
            isExecuting.set(false);
            flowsSequencer.flowFinished(FlowType.WARMUP_DEMOTER, demoteContext.getFlowId(), true);
            fireEventDemoteEnd(true);
            demoteContext = null;
        }
        else {
            logger.error("demote end was called by another procces (demoteSequence = %d), or demote was canceled", demoteSequence);
        }
    }

    private void fireEventDemoteEnd(boolean success)
    {
        logger.debug("fire event demote end");

        if (demoteContext != null) {
            globalStatsDemoter.mergeStats(demoteContext.getStatsWarmupDemoter());
            globalStatsDemoter.addnumber_of_cycles(demoteContext.getNumberOfCycles());
            WarmupDemoterFinishEvent event = new WarmupDemoterFinishEvent(demoteContext.getDemoterSequence(), success, demoteContext.getStatsWarmupDemoter().statsCounterMapper());
            eventBus.post(event);
        }
    }

    private void executeDemote(int demoterSequence)
            throws ExecutionException, InterruptedException
    {
        logger.debug("execute demote - demoteSequence = %d", demoterSequence);
        demoteContext.setFlowId(FlowIdGenerator.generateFlowId());
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        CompletableFuture<Boolean> future = flowsSequencer.tryRunningFlow(FlowType.WARMUP_DEMOTER, demoteContext.getFlowId(), Optional.empty());
        future.get();
        stopWatch.stop();
        globalStatsDemoter.addwaiting_for_lock_nano(stopWatch.getNanoTime());
        logger.debug("got key, start demote nano sec waited = %d", stopWatch.getNanoTime());
        logger.debug("%s: build tupleRank", catalogNameProvider.get());
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(tupleFilters, forceDeleteFailedObjects);
        demoteContext.setTupleRankList(tupleRankResult.tupleRankList());
        if (forceDeleteFailedObjects) {
            // In case of forceDeleteFailedObjects = false, failed objects will be deleted regulary (as dead objects \ low priority)
            logger.debug("%s: deleteFailedObjects = %d", catalogNameProvider.get(), tupleRankResult.failedObjects().size());
            deleteFailedObjects(tupleRankResult.failedObjects());
        }
        logger.debug("%s: deleteImmediateObjects = %d", catalogNameProvider.get(), tupleRankResult.immediateObjects().size());
        deleteImmediateObjects(tupleRankResult.immediateObjects());
        sortTupleRankCollection(demoteContext.getTupleRankList());
        tupleFilters = null;
    }

    void initDemoteContext(int demoterSequence)
    {
        isExecuting.set(true);
        lastExecutionTime = System.currentTimeMillis();
        int batchSize = Math.min(MAX_SUPPORTED_BATCH_SIZE, warmupDemoterConfig.getBatchSize());
        demoteContext = new DemoteContext(demoterSequence,
                                          warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                                          warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                                          batchSize,
                                          warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                                          warmupDemoterConfig.getEpsilon(),
                                          deleteEmptyRowGroups);
        logger.debug("%s: initDemoteContext: %s", catalogNameProvider.get(), demoteContext);
    }

    private void demoteCycleEnd()
    {
        DemoteStatus demoteStatus;
        if (demoteContext.getTupleRankList().isEmpty()) {
            this.highestPriority.set(0);
            demoteStatus = DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE;
            if (CollectionUtils.isEmpty(tupleFilters)) {
                resetHighestPriority();
            }
        }
        else if (reachedThreshold(demoteContext.getCleanupUsageThresholdPercentage())) {
            demoteStatus = DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
        }
        else {
            demoteStatus = DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD;
        }
        logger.debug("%s: demoteCycleEnd: call connectorSync.syncDemoteCycleEnd (demoteSequence = %d, lowestPriority = %f, highestPriority = %f, demoteStatus = %s)",
                     catalogNameProvider.get(), demoteContext.getDemoterSequence(), demoteContext.getLowestPriority(), highestPriority.get(), demoteStatus.name());
        connectorSync.syncDemoteEnd(demoteContext.getDemoterSequence(), demoteContext.getLowestPriority(), highestPriority.get(), demoteStatus);
    }

    void abortActiveDemote()
    {
        logger.error("demote was aborted, demoteContext = %s", demoteContext);
        deleteEmptyRowGroups = false;
        isExecuting.set(false);
        demoteContext = null;
        globalStatsDemoter.incnumber_of_runs_fail();
        fireEventDemoteEnd(false);
    }

    public void connectorSyncStartDemoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("%s: startDemoteCycle (maxPriorityToDemote = %f, isSingleConnector = %b) tupleRankList.size %d",
                     catalogNameProvider.get(), maxPriorityToDemote, isSingleConnector, demoteContext.getTupleRankList().size());
        try {
            maxPriorityToDemote = isSingleConnector ? Integer.MAX_VALUE : maxPriorityToDemote;
            demoteContext.increaseNumberOfCycles();
            if (!hasElementsToDemote(maxPriorityToDemote)) {
                logger.error("should not call demote to this connector");
            }
            boolean reachedThreshold = demoteCycle(maxPriorityToDemote, isSingleConnector);
            DemoteStatus demoteStatus;
            if (reachedThreshold) {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD;
            }
            else if (demoteContext.getTupleRankList().isEmpty()) {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE;
                if (CollectionUtils.isEmpty(tupleFilters)) {
                    resetHighestPriority();
                }
            }
            else {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
            }
            double lowestPriority = demoteContext.getLowestPriority();
            connectorSync.syncDemoteEnd(demoteContext.getDemoterSequence(), lowestPriority, highestPriority.get(), demoteStatus);
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                logger.error("%s: startDemoteCycle failed with error: %s", catalogNameProvider.get(), ((TrinoException) e).getErrorCode().getName());
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
        logger.debug("%s: demoteCycle: start demoteCycle", catalogNameProvider.get());
        while (shouldContinueDemote(reachedThreshold, isSingleConnector, elementsDeleted, maxPriorityToDemote)) {
            if (reachedThreshold(demoteContext.getCleanupUsageThresholdPercentage())) {
                long maxElemntsToDemote = isSingleConnector ? Integer.MAX_VALUE : demoteContext.getMaxElementsToDemote();
                long numberOfElementsToDemote = Math.min(maxElemntsToDemote - elementsDeleted, demoteContext.getBatchSize());
                long newElementsDeleted = deleteByTupleRank(numberOfElementsToDemote, maxPriorityToDemote);
                elementsDeleted += newElementsDeleted;
            }
            else {
                reachedThreshold = true;
                logger.warn("%s: demoteCycle: while loop: reached threshold", catalogNameProvider.get());
            }
        }
        logger.debug("%s: demoteCycle: tupleRankListSize = %d, elementsDeleted = %d, maxElementsToDemote = %d, maxPriorityToDemote = %f, lowestPriorityLeft = %f",
                     catalogNameProvider.get(), demoteContext.getTupleRankList().size(), elementsDeleted, demoteContext.getMaxElementsToDemote(), maxPriorityToDemote, demoteContext.getLowestPriority());
        return reachedThreshold;
    }

    private boolean shouldContinueDemote(boolean reachedThreshold, boolean isSingleConnector, long elementsDeleted, double maxPriorityToDemote)
    {
        if (reachedThreshold || demoteContext.getTupleRankList().isEmpty()) {
            return false;
        }
        if (isSingleConnector) {
            return true;
        }
        if (elementsDeleted < demoteContext.getMaxElementsToDemote() &&
                demoteContext.getTupleRankList().get(0).warmupProperties().priority() < maxPriorityToDemote) {
            return true;
        }
        return false;
    }

    private void validateInput()
    {
        if (warmupDemoterConfig.getBatchSize() < 1) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (warmupDemoterConfig.getEpsilon() <= 0) {
            throw new IllegalArgumentException("epsilon must be greater than 0");
        }
        if (warmupDemoterConfig.getMaxElementsToDemoteInIteration() < 1) {
            throw new IllegalArgumentException("maxElementsToDemote must be greater than 0");
        }
    }

    private boolean hasElementsToDemote(double priority)
    {
        return !demoteContext.getTupleRankList().isEmpty() && demoteContext.getLowestPriority() < priority;
    }

    private boolean reachedThreshold(double usageThresholdPercenatge)
    {
        return workerCapacityManager.getFractionCurrentUsageFromTotal() > parseFromPercentageToFraction(usageThresholdPercenatge);
    }

    private double parseFromPercentageToFraction(double percentage)
    {
        return percentage / 100;
    }

    public boolean canAllowDefaultWarmup()
    {
        return canAllowWarmup(defaultWarmupProperties.priority());
    }

    public boolean canAllowWarmup(double priority)
    {
        return priority >= (highestPriority.get() - warmupDemoterConfig.getWarmingPriorityAllowThreshold());
    }

    public boolean canAllowWarmup()
    {
        return !reachedThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage());
    }

    public synchronized AcquireWarmupStatus tryAllocateNativeResourceForWarmup()
    {
        workerCapacityManager.setCurrentUsage();
        workerCapacityManager.setExecutingTx(warpDeleteService.getNumActiveWarmingTasks());
        if (!canAllowWarmup()) {
            warpDeleteService.releaseTx();
            if (workerCapacityManager.getExecutingTxCount() <= 0) {
                tryDemoteStart();
            }
            return AcquireWarmupStatus.REACHED_THRESHOLD;
        }
        return AcquireWarmupStatus.SUCCESS;
    }

    @VisibleForTesting
    void deleteImmediateObjects(List<TupleRank> tupleRanks)
            throws ExecutionException, InterruptedException
    {
        logger.debug("start deleting %d immediate objects", tupleRanks.size());
        long deletedObjectsCount = warpDeleteService.delete(tupleRanks, demoteContext, deleteEmptyRowGroups);
        this.demoteContext.getStatsWarmupDemoter().adddead_objects_deleted(deletedObjectsCount);
    }

    @VisibleForTesting
    void deleteFailedObjects(List<TupleRank> failedObjects)
            throws ExecutionException, InterruptedException
    {
        logger.debug("start deleting %d failed objects", failedObjects.size());
        long deletedObjectsCount = warpDeleteService.delete(failedObjects, demoteContext, deleteEmptyRowGroups);
        WarmupDemoterStats statsWarmupDemoter = demoteContext.getStatsWarmupDemoter();
        statsWarmupDemoter.addfailed_objects_deleted(deletedObjectsCount);
    }

    @VisibleForTesting
    long deleteByTupleRank(long maxElementsToDemote, double maxPriorityToDemote)
            throws ExecutionException, InterruptedException
    {
        if (demoteContext.getTupleRankList().isEmpty()) {
            return 0;
        }
        int toIndex = 0;
        List<TupleRank> elementsToDemote = new ArrayList<>();
        while (demoteContext.getTupleRankList().size() > toIndex && toIndex < maxElementsToDemote &&
                demoteContext.getTupleRankList().get(toIndex).warmupProperties().priority() < maxPriorityToDemote) {
            elementsToDemote.add(demoteContext.getTupleRankList().get(toIndex));
            toIndex++;
        }
        long deletedObjectsCount = warpDeleteService.delete(elementsToDemote, demoteContext, deleteEmptyRowGroups);
        demoteContext.getTupleRankList().removeAll(elementsToDemote);
        demoteContext.getStatsWarmupDemoter().adddeleted_by_low_priority(deletedObjectsCount);
        double highestPriorityDeleted = elementsToDemote.get(elementsToDemote.size() - 1).warmupProperties().priority();
        highestPriority.set(highestPriorityDeleted);
        return deletedObjectsCount;
    }

    void sortTupleRankCollection(List<TupleRank> tupleRankList)
    {
        Collections.sort(tupleRankList);
    }

    public AtomicDouble getDemoterHighestPriority()
    {
        return highestPriority;
    }

    @VisibleForTesting
    public void resetHighestPriority()
    {
        this.highestPriority.set(0);
    }

    @VisibleForTesting
    boolean trySetIsExecutingToTrue()
    {
        return this.isExecuting.compareAndSet(false, true);
    }

    public boolean isExecuting()
    {
        return isExecuting.get();
    }

    @VisibleForTesting
    public int getCurrentRunSequence()
    {
        return demoteContext == null ? FAILED_DEMOTE_SQUENCE : demoteContext.getDemoterSequence();
    }

    public long getLastExecutionTime()
    {
        return lastExecutionTime;
    }

    public WarmupDemoterStats getCurrentRunStats()
    {
        return demoteContext == null ? null : demoteContext.getStatsWarmupDemoter();
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

    public void setResetHigestPriority(boolean resetHigestPriority)
    {
        this.resetHigestPriority = resetHigestPriority;
    }

    public void setDeleteEmptyRowGroups(boolean deleteEmptyRowGroups)
    {
        this.deleteEmptyRowGroups = deleteEmptyRowGroups;
    }

    public void setEnableDemote(boolean enableDemote)
    {
        this.enableDemote = enableDemote;
    }
}
