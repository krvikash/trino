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
            while (dataResource.getInProgressAddDataPagesRequests() > 0) {
                // busy looping is fine here as we expect in flight requests to finish fast
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
            }
            chunkManager.drainAllChunks();

            bufferNodeStateManager.transitionState(BufferNodeState.DRAINED);
        });
    }
}
