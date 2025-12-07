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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarpDeleteService.DeletionStats;
import io.trino.plugin.warp.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.warmup.WarmUtils.isEmptyCollection;
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
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.globalStatsDemoter = metricsManager.registerMetric(WarmupDemoterStats.create());
        this.demoterSync = requireNonNull(demoterSync);
        this.catalogName = requireNonNull(catalogName);
        this.eventBus = requireNonNull(eventBus);
        this.warpDeleteService = requireNonNull(warpDeleteService);

        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());

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
            logger.debug("catalog[%s]: is already executing (demoteContext = %s)", catalogName, demoteContext);
            globalStatsDemoter.incnot_executed_due_is_already_executing();
        }
        else {
            try {
                validateInput();
                isStarted = initiateDemoteProcess();
            }
            catch (Exception e) {
                shapingLogger.error(e, "catalog[%s]: initiateDemoteProcess failed", catalogName);
                globalStatsDemoter.incnumber_of_runs_fail();
            }
        }
        return isStarted;
    }

    boolean initiateDemoteProcess()
    {
        globalStatsDemoter.incnumber_of_calls();

        if (isEmptyCollection(tupleFilters)
                && !warmupDemoterConfig.isForceDeleteDeadObjects()
                && !warmupDemoterConfig.isForceDeleteFailedObjects()
                && !aboveThreshold(warmupDemoterConfig.getMaxUsageThresholdPercentage())) {
            logger.debug("catalog[%s]: not executing due thresholds", catalogName);
            globalStatsDemoter.incnot_executed_due_threshold();
            return false;
        }

        logger.debug("catalog[%s]: call start demote", catalogName);

        if (!demoterSync.tryStartDemoteProcess(demoteKey,
                warmupDemoterConfig.getMaxUsageThresholdPercentage(),
                warmupDemoterConfig.getCleanupUsageThresholdPercentage(),
                warmupDemoterConfig.getBatchSize(),
                warmupDemoterConfig.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfig.getEpsilon(),
                warmupDemoterConfig.isDeleteEmptyRowGroups(),
                warmupDemoterConfig.isForceDeleteFailedObjects(),
                warmupDemoterConfig.isResetHighestPriority())) {
            logger.debug("catalog[%s]: active demoter was initiated by another connector", catalogName);
            globalStatsDemoter.incnot_executed_due_sync_demote_start_rejected();
            return false;
        }
        logger.debug("catalog[%s]: call syncDemoteStart startDemote", catalogName);
        return true;
    }

    void connectorSyncStartDemote(
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups,
            boolean isForceDeleteFailedObjects,
            boolean isResetHighestPriority)
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
                    isDeleteEmptyRowGroups,
                    isForceDeleteFailedObjects,
                    isResetHighestPriority);

            demoteContext.get().statsWarmupDemoter().incnumber_of_runs();

            logger.debug("catalog[%s]: execute demote", catalogName);

            // In case of isForceDeleteFailedObjects = false, failed objects will be deleted regularly (as dead objects \ low priority)
            if (isForceDeleteFailedObjects) {
                deleteFailedObjects();
            }

            deleteImmediateObjects();

            tupleFilters = null;

            logger.debug("catalog[%s]: call demoteCycleEnd", catalogName);

            demoteCycleEnd();
        }
        catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                shapingLogger.error("catalog[%s]: startDemoteCycle failed with error: %s", catalogName, trinoException.getErrorCode().getName());
            }
            else {
                shapingLogger.error(e, "catalog[%s]: startDemoteCycle failed", catalogName);
            }
            cancelDemoteExecution();
        }
    }

    void cancelDemoteExecution()
    {
        shapingLogger.error("catalog[%s]: failed to execute demote, cancelling", catalogName);
        demoterSync.finishDemoteProcess(
                demoteKey,
                demoteContext.get().tupleRankResult().getLowestPriority(),
                demoteContext.get().highestPriorityDemoted().get(),
                DemoteStatus.REACHED_THRESHOLD);
        globalStatsDemoter.incnumber_of_runs_fail();
    }

    void connectorSyncDemoteEnd(double highestPriorityDemoted, boolean isFireEvent)
    {
        logger.debug("catalog[%s]: connectorSyncDemoteEnd(highestPriorityDemoted = %f)",
                catalogName, highestPriorityDemoted);
        demoteContext.get().highestPriorityDemoted().set(highestPriorityDemoted);

        logger.debug("catalog[%s]: connectorSyncDemoteEnd(isResetHighestPriority=%s, current highestPriorityDemoted = %f)",
                catalogName, demoteContext.get().isResetHighestPriority(), highestPriorityDemoted);

        if (isExecuting()) {
            globalStatsDemoter.mergeStats(demoteContext.get().statsWarmupDemoter());

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
                    demoteContext.get().statsWarmupDemoter().statsCounterMapper());
            logger.debug("catalog[%s]: fire event demote %s", catalogName, event);
            eventBus.post(event);
        }
    }

    @VisibleForTesting
    void initDemoteContext(
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups,
            boolean isForceDeleteFailedObjects,
            boolean isResetHighestPriority)
    {
        lastExecutionTime = System.currentTimeMillis();
        TupleRankResult tupleRankResult = warpDeleteService.buildTupleRank(tupleFilters, isForceDeleteFailedObjects);
        demoteContext.set(new DemoteContext(
                maxUsageThresholdPercentage,
                cleanupUsageThresholdPercentage,
                batchSize,
                maxElementsToDemoteInIteration,
                epsilon,
                isDeleteEmptyRowGroups,
                isResetHighestPriority,
                tupleRankResult));
        logger.debug("catalog[%s]: initDemoteContext: %s", catalogName, demoteContext);
    }

    private void demoteCycleEnd()
    {
        logger.debug("catalog[%s]: demoteCycleEnd::start", catalogName);

        DemoteStatus demoteStatus = getDemoteStatus(
                !aboveThreshold(demoteContext.get().cleanupUsageThresholdPercentage()));

        logger.debug("catalog[%s]: demoteCycleEnd::finish demoteCycleEnd: call demoterSync.syncDemoteCycleEnd " +
                        "(lowestPriorityExist = %f, highestPriorityDemoted = %f, demoteStatus = %s)",
                catalogName,
                demoteContext.get().tupleRankResult().getLowestPriority(),
                demoteContext.get().highestPriorityDemoted().get(),
                demoteStatus);

        demoterSync.finishDemoteProcess(
                demoteKey,
                demoteContext.get().tupleRankResult().getLowestPriority(),
                demoteContext.get().highestPriorityDemoted().get(),
                demoteStatus);
    }

    void connectorSyncStartDemoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("catalog[%s]: connectorSyncStartDemoteCycle (maxPriorityToDemote = %f, isSingleConnector = %b) tupleRankList.size %d",
                catalogName, maxPriorityToDemote, isSingleConnector, demoteContext.get().tupleRankResult().tupleRankList().size());
        try {
            maxPriorityToDemote = isSingleConnector ? Integer.MAX_VALUE : maxPriorityToDemote;

            boolean belowThreshold = demoteCycle(maxPriorityToDemote, isSingleConnector);
            DemoteStatus demoteStatus = getDemoteStatus(belowThreshold);
            double lowestPriority = demoteContext.get().tupleRankResult().getLowestPriority();
            demoterSync.finishDemoteProcess(
                    demoteKey,
                    lowestPriority,
                    demoteContext.get().highestPriorityDemoted().get(),
                    demoteStatus);
        }
        catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                shapingLogger.error(e, "catalog[%s]: startDemoteCycle failed with error: %s", catalogName, trinoException.getErrorCode().getName());
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
        long maxElementsToDemote = isSingleConnector ? Integer.MAX_VALUE : demoteContext.get().maxElementsToDemote();
        long elementsDeleted = 0;
        long bytesDeleted = 0;
        logger.debug("catalog[%s]: demoteCycle: start demoteCycle", catalogName);
        while (aboveThreshold(demoteContext.get().cleanupUsageThresholdPercentage()) &&
                shouldContinueDemote(isSingleConnector, elementsDeleted, maxPriorityToDemote)) {
            long numberOfElementsToDemote = Math.min(maxElementsToDemote - elementsDeleted, demoteContext.get().batchSize());
            DeletionStats deletionStats = deleteByTupleRank(numberOfElementsToDemote, maxPriorityToDemote);
            elementsDeleted += deletionStats.objectCount();
            bytesDeleted += deletionStats.sizeInBytes();
        }
        logger.debug("catalog[%s]: demoteCycle: tupleRankListSize = %d, elementsDeleted = %d, bytesDeleted = %d, maxElementsToDemote = %d, maxPriorityToDemote = %f, lowestPriorityLeft = %f",
                catalogName,
                demoteContext.get().tupleRankResult().tupleRankList().size(),
                elementsDeleted,
                bytesDeleted,
                demoteContext.get().maxElementsToDemote(),
                maxPriorityToDemote,
                demoteContext.get().tupleRankResult().getLowestPriority());

        return !aboveThreshold(demoteContext.get().cleanupUsageThresholdPercentage());
    }

    private boolean shouldContinueDemote(boolean isSingleConnector, long elementsDeleted, double maxPriorityToDemote)
    {
        List<TupleRank> tupleRankList = demoteContext.get().tupleRankResult().tupleRankList();
        if (tupleRankList.isEmpty()) {
            return false;
        }

        if (isSingleConnector) {
            return true;
        }
        return elementsDeleted < demoteContext.get().maxElementsToDemote() &&
                tupleRankList.getFirst().warmupProperties().priority() < maxPriorityToDemote;
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

    private boolean aboveThreshold(double thresholdPercentage)
    {
        return workerCapacityManager.getFractionCurrentUsageFromTotal() > thresholdPercentage / 100;
    }

    public boolean canAllowWarmup(double priority)
    {
        return priority >= (demoterSync.getHighestPriorityDemoted().get() - warmupDemoterConfig.getWarmingPriorityAllowThreshold());
    }

    public boolean canAllowWarmup()
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

    private void deleteFailedObjects()
            throws ExecutionException, InterruptedException
    {
        List<TupleRank> failedObjects = demoteContext.get().tupleRankResult().failedObjects();
        logger.debug("catalog[%s]: deleteFailedObjects deleting %d failed objects", catalogName, failedObjects.size());
        DeletionStats deletionStats = failedObjects.isEmpty() ? DeletionStats.EMPTY :
                warpDeleteService.delete(failedObjects, demoteContext.get());
        WarmupDemoterStats statsWarmupDemoter = demoteContext.get().statsWarmupDemoter();
        statsWarmupDemoter.addfailed_objects_deleted(deletionStats.objectCount());
        statsWarmupDemoter.adddeleted_bytes(deletionStats.sizeInBytes());
    }

    private void deleteImmediateObjects()
            throws ExecutionException, InterruptedException
    {
        List<TupleRank> tupleRanks = demoteContext.get().tupleRankResult().immediateObjects();
        long numElementsDemoted = 0;
        long demotedBytes = 0;
        while (!tupleRanks.isEmpty() && aboveThreshold(demoteContext.get().cleanupUsageThresholdPercentage())) {
            DeletionStats deletionStats = deleteByMaxElementsToDemote(
                    tupleRanks,
                    demoteContext.get().maxElementsToDemote(),
                    _ -> true);
            numElementsDemoted += deletionStats.objectCount();
            demotedBytes += deletionStats.sizeInBytes();
        }

        logger.debug("catalog[%s]: deleteImmediateObjects -> deleted %d elements",
                catalogName, numElementsDemoted);
        demoteContext.get().statsWarmupDemoter().adddead_objects_deleted(numElementsDemoted);
        demoteContext.get().statsWarmupDemoter().adddeleted_bytes(demotedBytes);
    }

    private DeletionStats deleteByTupleRank(long maxElementsToDemote, double maxPriorityToDemote)
            throws ExecutionException, InterruptedException
    {
        DeletionStats deletionStats = deleteByMaxElementsToDemote(
                demoteContext.get().tupleRankResult().tupleRankList(),
                maxElementsToDemote,
                warmupProperties -> warmupProperties.priority() < maxPriorityToDemote);

        demoteContext.get().statsWarmupDemoter().adddeleted_by_low_priority(deletionStats.objectCount());
        demoteContext.get().statsWarmupDemoter().adddeleted_bytes(deletionStats.sizeInBytes());

        logger.debug("catalog[%s]: deleteByTupleRank -> deleted %d elements of total size %d bytes, highestPriorityDeleted=%s, maxPriorityToDemote=%s",
                catalogName,
                deletionStats.objectCount(),
                deletionStats.sizeInBytes(),
                demoteContext.get().highestPriorityDemoted(),
                maxPriorityToDemote);

        return deletionStats;
    }

    private DeletionStats deleteByMaxElementsToDemote(
            List<TupleRank> tupleRankList,
            long maxElementsToDemote,
            Predicate<WarmupProperties> predicate)
            throws ExecutionException, InterruptedException
    {
        if (tupleRankList.isEmpty()) {
            return DeletionStats.EMPTY;
        }
        int toIndex = 0;
        List<TupleRank> elementsToDemote = new ArrayList<>();
        while (tupleRankList.size() > toIndex && toIndex < maxElementsToDemote &&
                predicate.test(tupleRankList.get(toIndex).warmupProperties())) {
            elementsToDemote.add(tupleRankList.get(toIndex));
            toIndex++;
        }

        DeletionStats deletionStats = warpDeleteService.delete(elementsToDemote, demoteContext.get());

        tupleRankList.subList(0, elementsToDemote.size()).clear();

        double highestPriorityDeleted = elementsToDemote.getLast().warmupProperties().priority();

        demoteContext.get().highestPriorityDemoted().set(highestPriorityDeleted);

        return deletionStats;
    }

    public double getDemoterHighestPriority()
    {
        return demoterSync.getHighestPriorityDemoted().get();
    }

    private DemoteStatus getDemoteStatus(boolean belowThreshold)
    {
        DemoteStatus demoteStatus = DemoteStatus.NOT_COMPLETED;
        if (demoteContext.get().tupleRankResult().tupleRankList().isEmpty()) {
            demoteStatus = DemoteStatus.NO_ELEMENTS_TO_DEMOTE;
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
        return !isExecuting() ? null : demoteContext.get().statsWarmupDemoter();
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
