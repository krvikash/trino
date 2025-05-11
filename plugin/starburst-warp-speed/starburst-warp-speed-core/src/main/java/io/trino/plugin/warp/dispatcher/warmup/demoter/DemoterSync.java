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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
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
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.DemoteStatus.NOT_COMPLETED;
import static io.trino.plugin.warp.dispatcher.warmup.demoter.DemoteStatus.UNKNOWN;
import static io.trino.plugin.warp.storage.flows.FlowsSequencer.INVALID_FLOW_ID;
import static java.util.Objects.requireNonNull;

@Singleton
public class DemoterSync
{
    private static final Logger logger = Logger.get(DemoterSync.class);

    private final Map<Long, DemoteContext> demoterServiceContextMap;
    private final AtomicLong initiator;
    private final ExecutorService executorService;

    private final AtomicDouble highestPriorityDemoted = new AtomicDouble(0D);

    @Inject
    public DemoterSync()
    {
        executorService = Executors.newThreadPerTaskExecutor(daemonThreadsNamed("warp-speed-demoter-sync-%s"));
        demoterServiceContextMap = new ConcurrentHashMap<>();
        initiator = new AtomicLong(Long.MIN_VALUE);
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
        demoterServiceContextMap.remove(demoteKey);
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
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();

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
            logger.warn("demote process not allowed since this is not the not initiator");
            return;
        }

        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
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
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("catalog[%s]: finishDemoteProcess start demoteStatus[%s] lowestPriorityExist[%s] highestPriorityDemoted[%s]",
                catalogName, demoteStatus, lowestPriorityExist, highestPriorityDemoted);

        demoterServiceContextMap
                .computeIfPresent(demoteKey,
                        (_, demoteContext) -> new DemoteContext(
                                lowestPriorityExist,
                                highestPriorityDemoted,
                                demoteStatus,
                                demoteContext));

        logger.debug("catalog[%s]: finishDemoteProcess finish", catalogName);
    }

    // call flowStart on all demoter services so demoter process can start
    private void callDemoteFlowsStart()
            throws ExecutionException, InterruptedException
    {
        Futures.allAsList(demoterServiceContextMap
                        .keySet()
                        .stream()
                        .map(demoteKeyTmp -> Futures.submit(() -> this.flowStart(demoteKeyTmp), executorService))
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
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("catalog[%s]: callConnectorSyncStartDemote start ", catalogName);

        try {
            Futures.allAsList(demoterServiceContextMap
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().flowId() != INVALID_FLOW_ID)
                            .map(entry -> {
                                logger.debug("catalog[%s]: callConnectorSyncStartDemote start calling connectorSyncStartDemote catalog[%s]",
                                        catalogName,
                                        demoterServiceContextMap.get(entry.getKey()).catalogName);

                                return Futures.submit(
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
                                        executorService);
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
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
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
                                            demoterServiceContextMap.get(entry.getKey()).catalogName);
                                    markDemoteCycleStart(entry);
                                    return Futures.submit(() ->
                                                    entry.getValue()
                                                            .warmupDemoterService.connectorSyncStartDemoteCycle(
                                                                    minLowestPriorityExist + demoterServiceContextMap.get(demoteKey).warmupDemoterService.getEpsilon(),
                                                                    demoterServiceContextMap.size() == 1),
                                            executorService);
                                })
                                .toList())
                        .get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            //run initiator
            logger.debug("catalog[%s]: loopUntilNothingToDemote before calling initiator connectorSyncStartDemoteCycle", catalogName);
            demoterServiceContextMap
                    .get(demoteKey)
                    .warmupDemoterService.connectorSyncStartDemoteCycle(
                            minLowestPriorityExist + demoterServiceContextMap.get(demoteKey).warmupDemoterService.getEpsilon(),
                            demoterServiceContextMap.size() == 1);
        }
        logger.debug("catalog[%s]: loopUntilNothingToDemote finish", catalogName);
    }

    private void callConnectorSyncDemoteEnd(long demoteKey)
    {
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
                                        demoterServiceContextMap.get(demoteKey).catalogName,
                                        demoterServiceContextMap.get(entry.getKey()).catalogName,
                                        highestPriorityDemoted.get());
                                return Futures.submit(() ->
                                                entry.getValue().warmupDemoterService.connectorSyncDemoteEnd(highestPriorityDemoted.get(), false),
                                        executorService);
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
