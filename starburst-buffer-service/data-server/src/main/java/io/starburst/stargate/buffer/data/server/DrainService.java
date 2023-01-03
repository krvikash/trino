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

import io.airlift.log.Logger;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.execution.ChunkManager;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

/**
 * The service responsible for handling the DRAINING of the Server Node.
 * Upon finishing the draining process moves the server to DRAINED state.
 * Server in Drained state is ready for shutdown.
 */
public class DrainService
{
    private static final Logger LOG = Logger.get(DrainService.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("data-server-drain-service"));
    private final BufferNodeStateManager bufferNodeStateManager;
    private final DataResource dataResource;
    private final ChunkManager chunkManager;

    @Inject
    public DrainService(
            BufferNodeStateManager bufferNodeStateManager,
            DataResource dataResource,
            ChunkManager chunkManager)
    {
        this.bufferNodeStateManager = requireNonNull(bufferNodeStateManager, "bufferNodeStateManager is null");
        this.dataResource = requireNonNull(dataResource, "dataResource is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    public synchronized void drain()
    {
        if (bufferNodeStateManager.isDrainingStarted()) {
            return;
        }
        bufferNodeStateManager.transitionState(BufferNodeState.DRAINING);

        executor.submit(() -> {
            try {
                while (true) {
                    int inProgressAddDataPagesRequests = dataResource.getInProgressAddDataPagesRequests();
                    if (inProgressAddDataPagesRequests == 0) {
                        break;
                    }
                    LOG.info("Waiting until remaining %s in flight addData requests complete", inProgressAddDataPagesRequests);
                    // busy looping is fine here as we expect in flight requests to finish fast
                    try {
                        Thread.sleep(500);
                    }
                    catch (InterruptedException e) {
                        // ignore
                    }
                }
                chunkManager.drainAllChunks();
            }
            catch (Exception e) {
                LOG.error(e, "Unexpected failure while draining node");
            }
            // we mark node as DRAINED even on failure. It is not great but leaving node in DRAINING state
            // does not buy us anything and we will block external processes waiting for draining completion.
            bufferNodeStateManager.transitionState(BufferNodeState.DRAINED);
        });
    }
}
