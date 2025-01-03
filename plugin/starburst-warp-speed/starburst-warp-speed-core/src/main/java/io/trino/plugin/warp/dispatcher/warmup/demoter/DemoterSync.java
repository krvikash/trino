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

import com.google.common.util.concurrent.Futures;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.warp.log.ShapingLogger;
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
import static io.trino.plugin.warp.dispatcher.warmup.demoter.DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
import static java.util.Objects.requireNonNull;

public class DemoterSync
{
    private static final Logger logger = Logger.get(DemoterSync.class);

    private final Map<Long, DemoteContext> demoterServiceContextMap;
    private final AtomicLong initiator;
    private final ExecutorService executorService;
    private final ShapingLogger shapingLogger;

    @SuppressWarnings("StaticAssignmentInConstructor")
    public DemoterSync()
    {
        executorService = Executors.newThreadPerTaskExecutor(daemonThreadsNamed("warp-speed-demoter-sync-%s"));
        demoterServiceContextMap = new ConcurrentHashMap<>();
        initiator = new AtomicLong(Long.MIN_VALUE);
        shapingLogger = ShapingLogger.getInstance(logger, 1000, Duration.ofSeconds(60), 3); // cannot take it from GlobalConfig since this is across catalogs
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

    /// //////// demoter ///////////

    //called by demote initiator
    public boolean tryStartDemoteProcess(long demoteKey)
    {
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("prepare::start catalog[%s] demoteKey[%d], epsilon[%s]",
                catalogName, demoteKey, demoterServiceContextMap.get(demoteKey).warmupDemoterService.getEpsilon());

        // already running by another
        if ((initiator.get() != Long.MIN_VALUE) && (initiator.get() != demoteKey)) {
            return false;
        }

        if (initiator.compareAndExchange(Long.MIN_VALUE, demoteKey) == Long.MIN_VALUE) {
            logger.debug("prepare::finish catalog[%s]", catalogName);
            startDemoteProcess(demoteKey);
            return true;
        }

        shapingLogger.warn("catalog[%s] syncDemotePrepare::already running with another demote sequence, current[%s], initiator[%s]", catalogName, demoteKey, initiator.get());
        return false;
    }

    private void startDemoteProcess(long demoteKey)
    {
        if (initiator.get() != demoteKey) {
            shapingLogger.warn("demote process not allowed since this is not the not initiator");
            return;
        }

        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("start::start catalog[%s] demoteKey[%d]", catalogName, demoteKey);

        try {
            callDemoteFlowsStart();
        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        callConnectorSyncStartDemote(demoteKey);

        loopUntilNothingToDemote(demoteKey);

        // everyone is done
        logger.debug("all connectors are done catalog[%s]", catalogName);

        callConnectorSyncDemoteEnd(demoteKey);

        cleanupAfterDemoteProcess(catalogName);

        logger.debug("startDemote::finish catalog[%s] demoteKey[%d]", catalogName, demoteKey);
    }

    void finishDemoteProcess(
            long demoteKey,
            double lowestPriorityExist,
            double highestPriorityDemoted,
            DemoteStatus demoteStatus)
    {
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("finish::start catalog[%s] demoteStatus[%s] lowestPriorityExist[%s] highestPriorityDemoted[%s]",
                catalogName, demoteStatus, lowestPriorityExist, highestPriorityDemoted);

        demoterServiceContextMap
                .computeIfPresent(demoteKey,
                        (_, demoteContext) -> new DemoteContext(
                                lowestPriorityExist,
                                highestPriorityDemoted,
                                demoteStatus,
                                demoteContext));

        logger.debug("finish::finish catalog[%s]", catalogName);
    }

    // call flowStart on all demoter services so demoter process can start
    private void callDemoteFlowsStart()
            throws ExecutionException, InterruptedException
    {
        Futures.allAsList(demoterServiceContextMap
                        .keySet()
                        .stream()
                        .map(demoteKeyTmp -> {
                            logger.debug("calling connectorSyncStartDemote on catalog[%s]",
                                    demoterServiceContextMap.get(demoteKeyTmp).catalogName);

                            return Futures.submit(
                                    () -> this.flowStart(demoteKeyTmp),
                                    executorService);
                        })
                        .toList())
                .get();
    }

    private void callConnectorSyncStartDemote(long demoteKey)
    {
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("callConnectorSyncStartDemote::start catalog[%s]", catalogName);

        try {
            Futures.allAsList(demoterServiceContextMap
                            .entrySet()
                            .stream()
                            .map(entry -> {
                                logger.debug("callConnectorSyncStartDemote::start catalog[%s] calling connectorSyncStartDemote catalog[%s]",
                                        catalogName,
                                        demoterServiceContextMap.get(entry.getKey()).catalogName);

                                return Futures.submit(
                                        () -> {
                                            //mark demote start cycle
                                            demoterServiceContextMap
                                                    .computeIfPresent(entry.getKey(),
                                                            (_, demoteContext) -> new DemoteContext(
                                                                    DEMOTE_STATUS_NOT_COMPLETED,
                                                                    demoteContext));
                                            entry.getValue().warmupDemoterService.connectorSyncStartDemote();
                                        },
                                        executorService);
                            })
                            .toList())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        logger.debug("callConnectorSyncStartDemote::finish catalog[%s]", catalogName);
    }

    private void loopUntilNothingToDemote(long demoteKey)
    {
        CatalogName catalogName = demoterServiceContextMap.get(demoteKey).catalogName();
        logger.debug("loopUntilNothingToDemote::start catalog[%s] ", catalogName);

        //check the status of all demoter calls
        while (demoterServiceContextMap
                .entrySet()
                .stream()
                .anyMatch(entry -> DEMOTE_STATUS_NOT_COMPLETED.equals(entry.getValue().demoteStatus))) {
            logger.debug("loopUntilNothingToDemote:: catalog[%s] not all catalogs completed", catalogName);

            if (waitUntilAllFinished()) {
                logger.debug("loopUntilNothingToDemote:: catalog[%s] all demoter calls complete finished",
                        catalogName);
            }
            else {
                shapingLogger.warn("loopUntilNothingToDemote:: catalog[%s] NOT all demoter calls complete finished -> %s",
                        catalogName,
                        demoterServiceContextMap
                                .values()
                                .stream()
                                .filter(demoteContext -> DEMOTE_STATUS_NOT_COMPLETED.equals(demoteContext.demoteStatus()))
                                .toList());
            }

            double minLowestPriorityExist = getMinLowestPriorityExist();

            try {
                //run all non initiators
                Futures.allAsList(demoterServiceContextMap
                                .entrySet()
                                .stream()
                                .filter(entry -> !entry.getKey().equals(demoteKey)) //run all but the initiator
                                .map(entry -> {
                                    logger.debug("loopUntilNothingToDemote:: catalog[%s] before calling future connectorSyncStartDemoteCycle on catalog[%s]",
                                            catalogName,
                                            demoterServiceContextMap.get(entry.getKey()).catalogName);
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
            logger.debug("loopUntilNothingToDemote:: catalog[%s] before calling initiator connectorSyncStartDemoteCycle", catalogName);
            demoterServiceContextMap
                    .get(demoteKey)
                    .warmupDemoterService.connectorSyncStartDemoteCycle(
                            minLowestPriorityExist + demoterServiceContextMap.get(demoteKey).warmupDemoterService.getEpsilon(),
                            demoterServiceContextMap.size() == 1);
        }
        logger.debug("loopUntilNothingToDemote::finish catalog[%s] ", catalogName);
    }

    private void callConnectorSyncDemoteEnd(long demoteKey)
    {
        //find the highest priority that was demoted and set all on that value
        double maxHighestPriorityDemoted = getMaxHighestPriorityDemoted();

        try {
            Futures.allAsList(demoterServiceContextMap
                            .entrySet()
                            .stream()
                            .filter(entry -> !entry.getKey().equals(demoteKey))
                            .map(entry -> {
                                logger.debug("callConnectorSyncDemoteEnd:: catalog[%s] before calling future connectorSyncDemoteEnd on catalog[%s] with maxHighestPriorityDemoted=%s",
                                        demoterServiceContextMap.get(demoteKey).catalogName,
                                        demoterServiceContextMap.get(entry.getKey()).catalogName,
                                        maxHighestPriorityDemoted);
                                return Futures.submit(() ->
                                                entry.getValue().warmupDemoterService.connectorSyncDemoteEnd(maxHighestPriorityDemoted, false),
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
                .warmupDemoterService.connectorSyncDemoteEnd(maxHighestPriorityDemoted, true);
    }

    private void cleanupAfterDemoteProcess(CatalogName catalogName)
    {
        logger.debug("catalog[%s] finished demote flow, cleaning up", catalogName);

        demoterServiceContextMap.keySet().forEach(this::flowFinish);
        initiator.set(Long.MIN_VALUE);
    }

    private void flowStart(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("flowStart::start catalog[%s]", catalogName);

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
                        (_, demoteContextTmp) -> new DemoteContext(
                                demoteContextTmp.catalogName(),
                                demoteContextTmp.warmupDemoterService(),
                                demoteContextTmp.flowsSequencer(),
                                demoteContextTmp.lowestPriorityExist(),
                                demoteContextTmp.highestPriorityDemoted(),
                                demoteContextTmp.exclude(),
                                demoteContextTmp.demoteStatus(),
                                flowId,
                                demoteContextTmp.stopWatch()));

        logger.debug("flowStart::finish catalog[%s] got flowId[%s], start demote nano sec waited = %d",
                flowId, catalogName, stopWatch.getNanoTime());
    }

    private void flowFinish(long demoteKey)
    {
        DemoteContext demoteContext = demoterServiceContextMap.get(demoteKey);
        CatalogName catalogName = demoteContext.catalogName();
        logger.debug("flowFinish::start catalog[%s]", catalogName);

        demoteContext.flowsSequencer()
                .flowFinished(
                        FlowType.WARMUP_DEMOTER,
                        demoteContext.flowId(),
                        true);

        demoteContext.stopWatch().stop();

        resetDemoterContext(
                demoteKey,
                demoteContext.catalogName(),
                demoteContext.warmupDemoterService(),
                demoteContext.flowsSequencer());

        logger.debug("flowFinish::finish catalog[%s] took %s ms",
                catalogName,
                Duration.ofNanos(demoteContext.stopWatch().getNanoTime()).toMillis());
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
                .filter(demoteContext -> DEMOTE_STATUS_NOT_COMPLETED.equals(demoteContext.demoteStatus()))
                .map(DemoteContext::lowestPriorityExist)
                .min(Double::compareTo)
                .orElse(Double.MIN_VALUE);
    }

    private double getMaxHighestPriorityDemoted()
    {
        return demoterServiceContextMap
                .values()
                .stream()
                .map(DemoteContext::highestPriorityDemoted)
                .max(Double::compareTo)
                .orElseThrow();
    }

    private boolean waitUntilAllFinished()
    {
        return Failsafe.with(RetryPolicy.builder()
                        .withDelay(Duration.ofSeconds(1))
                        .withMaxDuration(Duration.ofSeconds(120))
                        .abortWhen(true)
                        .build())
                .get(() -> demoterServiceContextMap
                        .values()
                        .stream()
                        .noneMatch(demoteContext -> DEMOTE_STATUS_NOT_COMPLETED.equals(demoteContext.demoteStatus())));
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
                    null,
                    -1,
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
                    ", stopWatch=" + stopWatch.getTime() +
                    '}';
        }
    }
}
