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

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.DispatcherMetadata;
import io.trino.plugin.warp.dispatcher.WarpMDCContext;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.flows.FlowType;
import io.trino.plugin.warp.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.DemoteStatus.NOT_COMPLETED;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.DemoteStatus.UNKNOWN;
import static io.trino.plugin.warp.storage.flows.FlowsSequencer.INVALID_FLOW_ID;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@Singleton
public class DemoterSync
{
    private static final Logger logger = Logger.get(DemoterSync.class);
    private final ShapingLogger shapingLogger;

    private final Map<Long, DemoteContext> demoterServiceContextMap;
    private final AtomicLong initiator;

    private final AtomicDouble highestPriorityDemoted = new AtomicDouble(0d);

    @Inject
    public DemoterSync(ShapingLoggerFactory shapingLoggerFactory)
    {
        demoterServiceContextMap = new ConcurrentHashMap<>();
        initiator = new AtomicLong(Long.MIN_VALUE);
        this.shapingLogger = shapingLoggerFactory.getInstance(DispatcherMetadata.class);
    }

    public long registerCatalog(
            WarmupDemoterService warmupDemoterService,
            FlowsSequencer flowsSequencer,
            CatalogName catalogName,
            NodeManager nodeManager)
    {
        //we use nodeIdentifier since tests with multi-node are on the same jvm
        String nodeIdentifier = requireNonNull(nodeManager).getCurrentNode().getNodeIdentifier();
        long demoteKey = (catalogName + nodeIdentifier).hashCode();
        resetDemoterContext(demoteKey, catalogName, warmupDemoterService, flowsSequencer);
        return demoteKey;
    }

    public void unregisterCatalog(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.remove(demoteKey);
        if (demoteContext != null) {
            logger.debug("catalog[%s]: Unregistering. demoteKey[%d]", demoteContext.catalogName, demoteKey);

            ExecutorService executorService = demoteContext.executorService();
            executorService.shutdown();
            try (WarpMDCContext _ = new WarpMDCContext(demoteContext.catalogName.toString(), Optional.of("DEMOTER_SYNC"))) {
                if (!executorService.awaitTermination(5, MINUTES)) {
                    shapingLogger.warn("Executor did not terminate after 5 minutes, forcing termination. demoteKey[%d]", demoteKey);
                    executorService.shutdownNow();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            logger.debug("catalog[%s]: Finished unregistering. demoteKey[%d]", demoteContext.catalogName, demoteKey);
        }
    }

    private ListenableFuture<Void> submit(Runnable task, long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping task - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return Futures.immediateFuture(null);
        }

        try {
            return Futures.submit(task, demoteContext.executorService());
        }
        catch (RejectedExecutionException e) {
            if (demoterServiceContextMap.containsKey(demoteKey)) {
                shapingLogger.error(e, "Task rejected. demoteKey[%d]", demoteKey);
                throw e;
            }
            logger.debug(e, "Task rejected. Probably because demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return Futures.immediateFuture(null);
        }
    }

    public AtomicDouble getHighestPriorityDemoted()
    {
        return highestPriorityDemoted;
    }

    /// //////// demoter ///////////

    //called by demote initiator
    public boolean tryStartDemoteProcess(long demoteKey,
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups,
            boolean isForceDeleteFailedObjects,
            boolean isResetHighestPriority)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping tryStartDemoteProcess - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return false;
        }
        CatalogName catalogName = demoteContext.catalogName();

        logger.debug("catalog[%s]: tryStartDemoteProcess start - demoteKey[%d], epsilon[%s], isResetHighestPriority=%s",
                catalogName,
                demoteKey,
                epsilon,
                isResetHighestPriority);

        if (initiator.compareAndExchange(Long.MIN_VALUE, demoteKey) == Long.MIN_VALUE) {
            startDemoteProcess(
                    demoteKey,
                    maxUsageThresholdPercentage,
                    cleanupUsageThresholdPercentage,
                    batchSize,
                    maxElementsToDemoteInIteration,
                    epsilon,
                    isDeleteEmptyRowGroups,
                    isForceDeleteFailedObjects,
                    isResetHighestPriority);
            return true;
        }

        logger.debug("catalog[%s]: tryStartDemoteProcess finish", catalogName);
        return false;
    }

    private void startDemoteProcess(long demoteKey,
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups,
            boolean isForceDeleteFailedObjects,
            boolean isResetHighestPriority)
    {
        if (initiator.get() != demoteKey) {
            shapingLogger.warn("demote process not allowed since this is not the initiator");
            return;
        }

        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("demote process not allowed since demoteKey doesnt exists. demoteKey[%d]", demoteKey);
            return;
        }
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("catalog[%s]: startDemoteProcess start demoteKey[%d]", catalogName, demoteKey);

        try {
            callDemoteFlowsStart();
        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            callConnectorSyncStartDemote(
                    demoteKey,
                    maxUsageThresholdPercentage,
                    cleanupUsageThresholdPercentage,
                    batchSize,
                    maxElementsToDemoteInIteration,
                    epsilon,
                    isDeleteEmptyRowGroups,
                    isForceDeleteFailedObjects,
                    isResetHighestPriority);

            loopUntilNothingToDemote(demoteKey);

            // everyone is done
            logger.debug("catalog[%s]: all connectors are done", catalogName);

            callConnectorSyncDemoteEnd(demoteKey);
        }
        finally {
            cleanupAfterDemoteProcess(catalogName, isResetHighestPriority);
        }

        logger.debug("catalog[%s]: startDemoteProcess finish demoteKey[%d]", catalogName, demoteKey);
    }

    void finishDemoteProcess(
            long demoteKey,
            double lowestPriorityExist,
            double highestPriorityDemoted,
            DemoteStatus demoteStatus)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping finishDemoteProcess - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return;
        }
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("catalog[%s]: finishDemoteProcess start demoteStatus[%s] lowestPriorityExist[%s] highestPriorityDemoted[%s]",
                catalogName, demoteStatus, lowestPriorityExist, highestPriorityDemoted);

        demoterServiceContextMap
                .computeIfPresent(demoteKey,
                        (_, context) -> new DemoteContext(
                                lowestPriorityExist,
                                highestPriorityDemoted,
                                demoteStatus,
                                context));

        logger.debug("catalog[%s]: finishDemoteProcess finish", catalogName);
    }

    // call flowStart on all demoter services so demoter process can start
    private void callDemoteFlowsStart()
            throws ExecutionException, InterruptedException
    {
        Futures.allAsList(demoterServiceContextMap
                        .keySet()
                        .stream()
                        .map(demoteKeyTmp -> submit(() -> this.flowStart(demoteKeyTmp), demoteKeyTmp))
                        .toList())
                .get();
    }

    private void callConnectorSyncStartDemote(
            long demoteKey,
            double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemoteInIteration,
            double epsilon,
            boolean isDeleteEmptyRowGroups,
            boolean isForceDeleteFailedObjects,
            boolean isResetHighestPriority)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping callConnectorSyncStartDemote - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return;
        }
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("catalog[%s]: callConnectorSyncStartDemote start ", catalogName);

        try {
            Futures.allAsList(demoterServiceContextMap
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().flowId() != INVALID_FLOW_ID)
                            .map(entry -> {
                                logger.debug("catalog[%s]: callConnectorSyncStartDemote start calling connectorSyncStartDemote catalog[%s]",
                                        catalogName,
                                        entry.getValue().catalogName);

                                return submit(
                                        () -> {
                                            markDemoteCycleStart(entry);
                                            entry.getValue().warmupDemoterService.connectorSyncStartDemote(
                                                    maxUsageThresholdPercentage,
                                                    cleanupUsageThresholdPercentage,
                                                    batchSize,
                                                    maxElementsToDemoteInIteration,
                                                    epsilon,
                                                    isDeleteEmptyRowGroups,
                                                    isForceDeleteFailedObjects,
                                                    isResetHighestPriority);
                                        },
                                        entry.getKey());
                            })
                            .toList())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        logger.debug("catalog[%s]: callConnectorSyncStartDemote finish", catalogName);
    }

    private void markDemoteCycleStart(Map.Entry<Long, DemoteContext> entry)
    {
        //mark demote start cycle
        demoterServiceContextMap
                .computeIfPresent(entry.getKey(),
                        (_, demoteContext) -> new DemoteContext(
                                UNKNOWN,
                                demoteContext));
    }

    private void loopUntilNothingToDemote(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping loopUntilNothingToDemote - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return;
        }
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("catalog[%s]: loopUntilNothingToDemote start ", catalogName);

        //check the status of all demoter calls
        while (demoterServiceContextMap
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().flowId() != INVALID_FLOW_ID)
                .anyMatch(entry -> NOT_COMPLETED.equals(entry.getValue().demoteStatus))) {
            logger.debug("catalog[%s] loopUntilNothingToDemote not all catalogs completed", catalogName);

            double minLowestPriorityExist = getMinLowestPriorityExist();

            try {
                //run all non initiators
                Futures.allAsList(demoterServiceContextMap
                                .entrySet()
                                .stream()
                                .filter(entry -> entry.getValue().flowId() != INVALID_FLOW_ID)
                                .filter(entry -> !entry.getKey().equals(demoteKey)) //run all but the initiator
                                .filter(entry -> !DemoteStatus.NO_ELEMENTS_TO_DEMOTE.equals(demoterServiceContextMap.get(entry.getKey()).demoteStatus))
                                .map(entry -> {
                                    logger.debug("catalog[%s]: loopUntilNothingToDemote before calling future connectorSyncStartDemoteCycle on catalog[%s]",
                                            catalogName,
                                            entry.getValue().catalogName);
                                    markDemoteCycleStart(entry);
                                    return submit(
                                            () -> {
                                                DemoteContext context = demoterServiceContextMap.get(demoteKey);
                                                if (context == null) {
                                                    return;
                                                }
                                                entry.getValue()
                                                        .warmupDemoterService.connectorSyncStartDemoteCycle(
                                                                minLowestPriorityExist + context.warmupDemoterService.getEpsilon(),
                                                                demoterServiceContextMap.size() == 1);
                                            },
                                            entry.getKey());
                                })
                                .toList())
                        .get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            //run initiator
            demoteContext = demoterServiceContextMap.get(demoteKey);
            if (demoteContext == null) {
                logger.debug("aborting loopUntilNothingToDemote, demoteKey doesnt exists anymore. demoteKey[%d]", demoteKey);
                return;
            }
            logger.debug("catalog[%s]: loopUntilNothingToDemote before calling initiator connectorSyncStartDemoteCycle", catalogName);
            demoterServiceContextMap
                    .get(demoteKey)
                    .warmupDemoterService.connectorSyncStartDemoteCycle(
                            minLowestPriorityExist + demoteContext.warmupDemoterService.getEpsilon(),
                            demoterServiceContextMap.size() == 1);
        }
        logger.debug("catalog[%s]: loopUntilNothingToDemote finish", catalogName);
    }

    private void callConnectorSyncDemoteEnd(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping callConnectorSyncDemoteEnd - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return;
        }

        //find the highest priority that was demoted and set all on that value
        highestPriorityDemoted.set(getMaxHighestPriorityDemoted());

        try {
            Futures.allAsList(demoterServiceContextMap
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().flowId() != INVALID_FLOW_ID)
                            .filter(entry -> !entry.getKey().equals(demoteKey))
                            .map(entry -> {
                                logger.debug("catalog[%s]: callConnectorSyncDemoteEnd before calling future connectorSyncDemoteEnd on catalog[%s] with maxHighestPriorityDemoted=%s",
                                        demoteContext.catalogName,
                                        entry.getValue().catalogName,
                                        highestPriorityDemoted.get());
                                return submit(() ->
                                                entry.getValue().warmupDemoterService.connectorSyncDemoteEnd(highestPriorityDemoted.get(), false),
                                        entry.getKey());
                            })
                            .toList())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        demoterServiceContextMap
                .get(demoteKey)
                .warmupDemoterService.connectorSyncDemoteEnd(highestPriorityDemoted.get(), true);
    }

    private void cleanupAfterDemoteProcess(CatalogName catalogName, boolean isResetHighestPriority)
    {
        logger.debug("catalog[%s] finished demote flow, cleaning up", catalogName);

        demoterServiceContextMap.entrySet().stream()
                .filter(entry -> entry.getValue().flowId() != INVALID_FLOW_ID)
                .forEach(entry -> flowFinish(entry.getKey()));

        if (isResetHighestPriority) {
            highestPriorityDemoted.set(0);
        }

        initiator.set(Long.MIN_VALUE);
    }

    private void flowStart(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping flowStart - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return;
        }
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("catalog[%s]: flowStart start", catalogName);

        demoteContext.stopWatch().start();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        long flowId;
        try {
            flowId = demoteContext.flowsSequencer()
                    .tryRunningFlow(FlowType.WARMUP_DEMOTER, Optional.empty())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        finally {
            stopWatch.stop();
        }

        demoterServiceContextMap
                .computeIfPresent(demoteKey,
                        (_, demoteContextTmp) -> new DemoteContext(flowId, demoteContextTmp));

        logger.debug("catalog[%s]: flowStart finish got flowId[%s], start demote nano sec waited = %d",
                catalogName, flowId, stopWatch.getNanoTime());
    }

    private void flowFinish(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        if (demoteContext == null) {
            logger.debug("Skipping flowFinish - demoteKey doesn't exists. demoteKey[%d]", demoteKey);
            return;
        }
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("catalog[%s]: flowFinish start for flowId[%s]", catalogName, demoteContext.flowId());

        demoteContext.flowsSequencer()
                .flowFinished(
                        FlowType.WARMUP_DEMOTER,
                        demoteContext.flowId(),
                        true);

        if (demoteContext.stopWatch().isStarted()) {
            demoteContext.stopWatch().stop();
        }

        resetDemoterContext(
                demoteKey,
                demoteContext.catalogName(),
                demoteContext.warmupDemoterService(),
                demoteContext.flowsSequencer());

        logger.debug("catalog[%s]: flowFinish finish for flowId[%s] took %s ms",
                catalogName,
                demoteContext.flowId(),
                Duration.ofNanos(demoteContext.stopWatch().getNanoTime()).toMillis());
        demoteContext.stopWatch().reset();
    }

    private void resetDemoterContext(
            long demoteKey,
            CatalogName catalogName,
            WarmupDemoterService warmupDemoterService,
            FlowsSequencer flowsSequencer)
    {
        demoterServiceContextMap.put(demoteKey,
                new DemoteContext(
                        catalogName,
                        warmupDemoterService,
                        flowsSequencer));
    }

    private Double getMinLowestPriorityExist()
    {
        return demoterServiceContextMap
                .values()
                .stream()
                .filter(demoteContext -> demoteContext.flowId() != INVALID_FLOW_ID)
                .filter(demoteContext -> NOT_COMPLETED.equals(demoteContext.demoteStatus()))
                .map(DemoteContext::lowestPriorityExist)
                .min(Double::compareTo)
                .orElse(Double.MIN_VALUE);
    }

    private double getMaxHighestPriorityDemoted()
    {
        return demoterServiceContextMap
                .values()
                .stream()
                .filter(demoteContext -> demoteContext.flowId() != INVALID_FLOW_ID)
                .map(DemoteContext::highestPriorityDemoted)
                .max(Double::compareTo)
                .orElseThrow();
    }

    record DemoteContext(
            CatalogName catalogName,
            ExecutorService executorService,
            WarmupDemoterService warmupDemoterService,
            FlowsSequencer flowsSequencer,
            double lowestPriorityExist,
            double highestPriorityDemoted,
            boolean exclude,
            DemoteStatus demoteStatus,
            long flowId,
            StopWatch stopWatch)
    {
        DemoteContext(CatalogName catalogName,
                WarmupDemoterService warmupDemoterService,
                FlowsSequencer flowsSequencer)
        {
            this(catalogName,
                    Executors.newThreadPerTaskExecutor(daemonThreadsNamed(format("warp-speed-demoter-sync-%s", catalogName) + "-%s")),
                    warmupDemoterService,
                    flowsSequencer,
                    -1,
                    -1,
                    false,
                    UNKNOWN,
                    INVALID_FLOW_ID,
                    new StopWatch());
        }

        DemoteContext(DemoteStatus demoteStatus, DemoteContext demoteContext)
        {
            this(demoteContext.catalogName(),
                    demoteContext.executorService(),
                    demoteContext.warmupDemoterService(),
                    demoteContext.flowsSequencer(),
                    demoteContext.lowestPriorityExist(),
                    demoteContext.highestPriorityDemoted(),
                    demoteContext.exclude(),
                    demoteStatus,
                    demoteContext.flowId(),
                    demoteContext.stopWatch());
        }

        DemoteContext(double lowestPriorityExist,
                double highestPriorityDemoted,
                DemoteStatus demoteStatus,
                DemoteContext demoteContext)
        {
            this(demoteContext.catalogName(),
                    demoteContext.executorService(),
                    demoteContext.warmupDemoterService(),
                    demoteContext.flowsSequencer(),
                    lowestPriorityExist,
                    highestPriorityDemoted,
                    demoteContext.exclude(),
                    demoteStatus,
                    demoteContext.flowId(),
                    demoteContext.stopWatch());
        }

        DemoteContext(long flowId, DemoteContext demoteContext)
        {
            this(demoteContext.catalogName(),
                    demoteContext.executorService(),
                    demoteContext.warmupDemoterService(),
                    demoteContext.flowsSequencer(),
                    demoteContext.lowestPriorityExist(),
                    demoteContext.highestPriorityDemoted(),
                    demoteContext.exclude(),
                    demoteContext.demoteStatus(),
                    flowId,
                    demoteContext.stopWatch());
        }

        @Override
        public String toString()
        {
            return "DemoteContext{" +
                    "catalogName=" + catalogName +
                    ", lowestPriorityExist=" + lowestPriorityExist +
                    ", highestPriorityDemoted=" + highestPriorityDemoted +
                    ", exclude=" + exclude +
                    ", demoteStatus=" + demoteStatus +
                    ", flowId=" + flowId +
                    '}';
        }
    }
}
