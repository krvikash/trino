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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Key;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.QueryManager;
import io.trino.node.NodeState;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.testing.TestingTrinoServer.TestShutdownAction;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.execution.QueryState.FINISHED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestEmbeddedBufferGracefulShutdown
{
    private static final long TEST_TIMEOUT_MILLIS = 240_000;
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 120_000;
    private static final long BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS = 30_000;
    private static final long TRACKED_EXCHANGES_TIMEOUT_MILLIS = 30_000;

    @Test
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testEmbeddedBufferDrainsOnWorkerShutdown()
            throws Exception
    {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
        try (DistributedQueryRunner queryRunner = createBufferNodeQueryRunner(2)) {
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
            // Wait until the worker's buffer actually has exchange data — this confirms the
            assertEventually(new Duration(TRACKED_EXCHANGES_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(chunkManager.getTrackedExchanges())
                            .describedAs("Expected exchanges registered on the worker's buffer")
                            .isGreaterThan(0));
            // Verify queries are still running before we trigger shutdown — exchange data is in flight
            assertThat(Futures.allAsList(queryFutures).isDone())
                    .describedAs("Queries should still be running when shutdown is triggered")
                    .isFalse();
            assertThat(chunkManager.getSpooledChunksCount())
                    .describedAs("No chunks spooled to persistent storage before node drain")
                    .isEqualTo(0);

            // Trigger worker shutdown while the buffer holds data
            worker.getNodeStateManager().transitionState(NodeState.SHUTTING_DOWN);

            // Verify the buffer transitions to DRAINING — chunks are being spooled to persistent storage
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINING));

            // Verify buffer reaches DRAINED — all chunks spooled, none remain in memory
            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));

            assertThat(chunkManager.getOpenChunks() + chunkManager.getClosedChunks())
                    .describedAs("All in-memory chunks should be spooled after drain")
                    .isEqualTo(0);
            assertThat(chunkManager.getSpooledChunksCount())
                    .describedAs("Chunks should have been spooled to persistent storage during drain")
                    .isGreaterThan(0);

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
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testEmbeddedBufferStateAfterIdleWorkerShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createBufferNodeQueryRunner(1)) {
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
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testEmbeddedBufferOnSingleNodeShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createBufferNodeQueryRunner(0)) {
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

    private static TestingTrinoServer getWorker(DistributedQueryRunner queryRunner)
    {
        return queryRunner.getServers()
                .stream()
                .filter(server -> !server.isCoordinator())
                .findFirst()
                .orElseThrow();
    }

    private static DistributedQueryRunner createBufferNodeQueryRunner(int workerCount)
            throws Exception
    {
        Map<String, String> extraProperties = getFaultTolerantExecutionExtraProperties();
        ImmutableMap<String, String> exchangeManagerProperties = getExchangeManagerProperties();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .addCoordinatorProperty("node-scheduler.include-coordinator", "true")
                .setExtraProperties(extraProperties)
                .setWorkerCount(workerCount)
                .withExchange("buffer", exchangeManagerProperties)
                .build();

        // Install mock connector — its splits have remotelyAccessible=true (SPI default),
        // unlike tpch/memory splits which are pinned to specific nodes and cause
        // "No nodes available" errors when a worker shuts down during FTE task retries.
        queryRunner.installPlugin(new MockConnectorPlugin(
                MockConnectorFactory.builder()
                        .withGetColumns(schemaTableName -> ImmutableList.of(
                                new ColumnMetadata("id", BIGINT),
                                new ColumnMetadata("group_key", VARCHAR)))
                        .withData(schemaTableName -> {
                            ImmutableList.Builder<List<?>> rows = ImmutableList.builder();
                            for (int i = 0; i < 5000; i++) {
                                rows.add(ImmutableList.of((long) i, "group_" + (i % 100)));
                            }
                            return rows.build();
                        })
                        .build()));
        queryRunner.createCatalog("mock", "mock");

        return queryRunner;
    }

    private static ImmutableMap<String, String> getExchangeManagerProperties()
    {
        // Use 1 buffer node per partition so exchange data is NOT replicated across nodes.
        // This ensures partitions assigned to a specific buffer node can only be read
        // from that node (or from spooled storage after it drains).
        return ImmutableMap.<String, String>builder()
                .put("exchange.use-embedded-buffer-service", "true")
                .put("exchange.sink-target-written-pages-count", "3")
                .put("exchange.source-handle-target-chunks-count", "4")
                .put("exchange.min-base-buffer-nodes-per-partition", "1")
                .put("exchange.max-base-buffer-nodes-per-partition", "1")
                .buildOrThrow();
    }

    private static Map<String, String> getFaultTolerantExecutionExtraProperties()
            throws IOException
    {
        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        File exchangeManagerDirectory = createTempDirectory("exchange_manager").toFile();
        extraProperties.put("embedded-buffer-service-enabled", "true");
        extraProperties.put("buffer.spooling.directory", exchangeManagerDirectory.getAbsolutePath());
        extraProperties.put("buffer.testing.allow-local-spooling", "true");
        extraProperties.put("buffer.draining.min-duration", "5s");
        extraProperties.put("query.executor-pool-size", "10");
        extraProperties.put("shutdown.grace-period", "1s");
        return extraProperties;
    }
}
