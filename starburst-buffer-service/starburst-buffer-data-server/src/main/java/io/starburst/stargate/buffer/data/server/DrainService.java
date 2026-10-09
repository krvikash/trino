/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import jakarta.annotation.PreDestroy;

import java.lang.management.ThreadInfo;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The service responsible for handling the DRAINING of the Server Node.
 * Upon finishing the draining process moves the server to DRAINED state.
 * Server in Drained state is ready for shutdown.
 */
public class DrainService
{
    private static final Logger log = Logger.get(DrainService.class);
    private static final Duration DEFAULT_MAX_WAIT_NO_IN_PROGRESS_ADD_DATA_PAGES_REQUESTS = Duration.succinctDuration(2, TimeUnit.MINUTES);
    private static final Duration DEFAULT_NO_PROGRESS_THREAD_DUMP_THRESHOLD = Duration.succinctDuration(30, TimeUnit.SECONDS);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("data-server-drain-service"));
    private final Duration drainAllChunksTimeout;
    private final Duration maxWaitNoInProgressAddDataPagesRequests;
    private final Duration noProgressThreadDumpThreshold;
    private final BufferNodeStateManager bufferNodeStateManager;
    private final AddDataPagesInProgressTracker inProgressTracker;
    private final ChunkManager chunkManager;
    private final Optional<DiscoveryBroadcast> discoveryBroadcast;

    @Inject
    public DrainService(
            DataServerConfig dataServerConfig,
            BufferNodeStateManager bufferNodeStateManager,
            AddDataPagesInProgressTracker inProgressTracker,
            ChunkManager chunkManager,
            Optional<DiscoveryBroadcast> discoveryBroadcast)
    {
        this(dataServerConfig,
                bufferNodeStateManager,
                inProgressTracker,
                chunkManager,
                discoveryBroadcast,
                DEFAULT_MAX_WAIT_NO_IN_PROGRESS_ADD_DATA_PAGES_REQUESTS,
                DEFAULT_NO_PROGRESS_THREAD_DUMP_THRESHOLD);
    }

    @VisibleForTesting
    DrainService(
            DataServerConfig dataServerConfig,
            BufferNodeStateManager bufferNodeStateManager,
            AddDataPagesInProgressTracker inProgressTracker,
            ChunkManager chunkManager,
            Optional<DiscoveryBroadcast> discoveryBroadcast,
            Duration maxWaitNoInProgressAddDataPagesRequests,
            Duration noProgressThreadDumpThreshold)
    {
        this.drainAllChunksTimeout = requireNonNull(dataServerConfig, "dataServerConfig is null").getDrainAllChunksTimeout();
        this.bufferNodeStateManager = requireNonNull(bufferNodeStateManager, "bufferNodeStateManager is null");
        this.inProgressTracker = requireNonNull(inProgressTracker, "inProgressTracker is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.discoveryBroadcast = requireNonNull(discoveryBroadcast, "discoveryBroadcast is null");
        this.maxWaitNoInProgressAddDataPagesRequests = requireNonNull(maxWaitNoInProgressAddDataPagesRequests, "maxWaitNoInProgressAddDataPagesRequests is null");
        this.noProgressThreadDumpThreshold = requireNonNull(noProgressThreadDumpThreshold, "noProgressThreadDumpThreshold is null");
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    public void awaitDrain()
    {
        drain();
        long deadlineMillis = System.currentTimeMillis() + maxWaitNoInProgressAddDataPagesRequests.toMillis() + drainAllChunksTimeout.toMillis();
        while (bufferNodeStateManager.getState() != BufferNodeState.DRAINED) {
            if (System.currentTimeMillis() > deadlineMillis) {
                log.warn("Timed out waiting for buffer node to reach DRAINED state");
                return;
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                log.warn("Interrupted while waiting for buffer node to reach DRAINED state");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public synchronized void drain()
    {
        log.info("Draining buffer node has been requested");
        if (bufferNodeStateManager.isDrainingStarted()) {
            return;
        }
        long drainStartMillis = System.currentTimeMillis();
        bufferNodeStateManager.transitionState(BufferNodeState.DRAINING);
        discoveryBroadcast.ifPresent(DiscoveryBroadcast::broadcast);

        executor.submit(() -> {
            try {
                boolean success = waitNoInProgressAddDataPagesRequests();
                if (!success) {
                    log.error("HACK: draining all chunks even though addDataPages in progress reported");
                }
                else {
                    log.info("All in flight addData requests completed, proceeding to drain all chunks");
                }
                chunkManager.drainAllChunks();
                log.info("Buffer node drained successfully in %s", Duration.succinctDuration(System.currentTimeMillis() - drainStartMillis, MILLISECONDS));
            }
            catch (Exception e) {
                log.error(e, "Unexpected failure while draining node after %s", Duration.succinctDuration(System.currentTimeMillis() - drainStartMillis, MILLISECONDS));
            }

            // we mark node as DRAINED even on failure. It is not great but leaving node in DRAINING state
            // does not buy us anything and we will block external processes waiting for draining completion.
            bufferNodeStateManager.transitionState(BufferNodeState.DRAINED);
            discoveryBroadcast.ifPresent(DiscoveryBroadcast::broadcast);
        });
    }

    private boolean waitNoInProgressAddDataPagesRequests()
    {
        long waitStart = System.currentTimeMillis();
        int lastInProgressAddDataPagesRequests = Integer.MAX_VALUE;
        long lastProgressMillis = waitStart;
        boolean threadDumpLogged = false;
        while (true) {
            int inProgressAddDataPagesRequests = inProgressTracker.getInProgressAddDataPagesRequests();
            if (inProgressAddDataPagesRequests == 0) {
                log.info("No more remaining in flight addData requests");
                break;
            }
            long now = System.currentTimeMillis();
            if (inProgressAddDataPagesRequests < lastInProgressAddDataPagesRequests) {
                lastInProgressAddDataPagesRequests = inProgressAddDataPagesRequests;
                lastProgressMillis = now;
            }
            else if (!threadDumpLogged && now - lastProgressMillis > noProgressThreadDumpThreshold.toMillis()) {
                log.warn("No in flight addData request completed in %s; %s remaining".formatted(noProgressThreadDumpThreshold, inProgressAddDataPagesRequests));
                logInProgressRequests();
                logThreadDump();
                threadDumpLogged = true;
            }
            if (now > waitStart + maxWaitNoInProgressAddDataPagesRequests.toMillis()) {
                chunkManager.logAddDataPagesInProgressDebugInfo();
                logInProgressRequests();
                log.error("Still %s in flight addData requests after waiting %s".formatted(inProgressAddDataPagesRequests, maxWaitNoInProgressAddDataPagesRequests));
                return false;
            }
            log.info("Waiting until remaining %s in flight addData requests complete", inProgressAddDataPagesRequests);
            // busy looping is fine here as we expect in flight requests to finish fast
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                // ignore
            }
        }
        return true;
    }

    private void logInProgressRequests()
    {
        inProgressTracker.describeInProgressRequests()
                .forEach(request -> log.info("In flight addData request: %s", request));
    }

    private static void logThreadDump()
    {
        try {
            // one entry per thread
            for (ThreadInfo thread : getThreadMXBean().dumpAllThreads(true, true)) {
                log.info("Thread dump: %s", formatThread(thread));
            }
        }
        catch (RuntimeException e) {
            // diagnostics must never prevent chunks from being drained
            log.warn(e, "Failed to dump threads");
        }
    }

    private static String formatThread(ThreadInfo thread)
    {
        StringBuilder builder = new StringBuilder()
                .append('"').append(thread.getThreadName()).append('"')
                .append(" id=").append(thread.getThreadId())
                .append(' ').append(thread.getThreadState());
        if (thread.getLockName() != null) {
            builder.append(" on ").append(thread.getLockName());
        }
        if (thread.getLockOwnerName() != null) {
            builder.append(" owned by \"").append(thread.getLockOwnerName()).append("\" id=").append(thread.getLockOwnerId());
        }
        for (StackTraceElement element : thread.getStackTrace()) {
            builder.append("\n\tat ").append(element);
        }
        return builder.toString();
    }
}
