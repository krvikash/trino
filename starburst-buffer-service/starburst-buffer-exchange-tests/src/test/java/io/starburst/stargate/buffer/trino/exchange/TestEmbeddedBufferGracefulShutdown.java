/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Key;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.trino.execution.QueryManager;
import io.trino.node.NodeState;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.testing.TestingTrinoServer.TestShutdownAction;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;

import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.createRunnerWithWorkers;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.createSingleNodeRunner;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.getWorker;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestEmbeddedBufferGracefulShutdown
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 120_000;

    @Test
    @Timeout(value = EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testEmbeddedBufferDrainsOnWorkerShutdown()
            throws Exception
    {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
        try (DistributedQueryRunner queryRunner = createRunnerWithWorkers(2)) {
            TestingTrinoServer worker = getWorker(queryRunner);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();

            BufferNodeStateManager bufferNodeStateManager = worker.getInstance(Key.get(BufferNodeStateManager.class));
            ChunkManager chunkManager = worker.getInstance(Key.get(ChunkManager.class));
            // Wait for buffer to become ACTIVE (transition happens async via DiscoveryBroadcast)
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE));

            // Submit multiple queries asynchronously to produce exchange data on the worker.
            // Uses mock connector whose splits are remotelyAccessible=true (SPI default),
            // allowing FTE to retry tasks on a different worker after shutdown.
            List<ListenableFuture<MaterializedResult>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(
                        () -> queryRunner.execute(
                                "SELECT COUNT(*), a.group_key " +
                                        "FROM mock.default.test_table a, mock.default.test_table b " +
                                        "GROUP BY a.group_key")));
            }
            // Verify queries are still running - exchange data is in flight
            assertThat(Futures.allAsList(queryFutures).isDone())
                    .describedAs("Queries should still be running when shutdown is triggered")
                    .isFalse();

            // Trigger worker shutdown while queries are running
            worker.getNodeStateManager().transitionState(NodeState.SHUTTING_DOWN);

            // Verify the buffer transitions to DRAINING — chunks are being spooled to persistent storage
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINING));

            // Verify buffer reaches DRAINED - drain completed, no in-memory chunks remain
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));

            assertThat(chunkManager.getOpenChunks() + chunkManager.getClosedChunks())
                    .describedAs("All in-memory chunks should be gone after drain")
                    .isEqualTo(0);

            // All queries must complete successfully.
            Futures.allAsList(queryFutures).get();
            List<BasicQueryInfo> queryInfos = queryManager.getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertThat(info.getState()).isEqualTo(FINISHED);
            }

            // Verify the worker actually shut down
            TestShutdownAction shutdownAction = (TestShutdownAction) worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertThat(shutdownAction.isWorkerShutdown()).isTrue();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(value = EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testEmbeddedBufferStateAfterIdleWorkerShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createRunnerWithWorkers(1)) {
            TestingTrinoServer worker = getWorker(queryRunner);

            BufferNodeStateManager bufferNodeStateManager = worker.getInstance(Key.get(BufferNodeStateManager.class));
            // Wait for buffer to become ACTIVE (transition happens async via DiscoveryBroadcast)
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE));

            // Shut down an idle worker (no active queries)
            worker.getNodeStateManager().transitionState(NodeState.SHUTTING_DOWN);

            TestShutdownAction shutdownAction = (TestShutdownAction) worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertThat(shutdownAction.isWorkerShutdown()).isTrue();

            // Verify buffer state after shutdown of idle worker
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));
        }
    }

    @Test
    @Timeout(value = EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testEmbeddedBufferOnSingleNodeShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createSingleNodeRunner()) {
            TestingTrinoServer coordinator = queryRunner.getCoordinator();

            BufferNodeStateManager bufferNodeStateManager = coordinator.getInstance(Key.get(BufferNodeStateManager.class));
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE));

            // Run a query so the coordinator-worker exercises the embedded buffer
            queryRunner.execute("SELECT COUNT(*), group_key FROM mock.default.test_table GROUP BY group_key");

            List<BasicQueryInfo> queryInfos = coordinator.getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertThat(info.getState()).isEqualTo(FINISHED);
            }

            // Trigger coordinator shutdown — should close the embedded buffer without errors
            coordinator.close();
        }
    }
}
