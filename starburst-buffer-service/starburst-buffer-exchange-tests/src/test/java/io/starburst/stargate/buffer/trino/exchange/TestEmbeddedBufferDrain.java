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
import io.starburst.stargate.buffer.data.server.DrainService;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;

import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.CHUNKS_AVAILABLE_TIMEOUT_MILLIS;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.createRunnerWithWorkers;
import static io.starburst.stargate.buffer.trino.exchange.EmbeddedBufferQueryRunner.getWorker;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestEmbeddedBufferDrain
{
    @Test
    @Timeout(value = EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testDrainWithNoData()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createRunnerWithWorkers(2)) {
            TestingTrinoServer worker = getWorker(queryRunner);
            BufferNodeStateManager bufferNodeStateManager = worker.getInstance(Key.get(BufferNodeStateManager.class));
            ChunkManager chunkManager = worker.getInstance(Key.get(ChunkManager.class));
            DrainService drainService = worker.getInstance(Key.get(DrainService.class));

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE));

            drainService.drain();

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINING));

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));

            assertThat(chunkManager.getSpooledChunksCount())
                    .describedAs("No data should have been spooled when draining without active queries")
                    .isEqualTo(0);
        }
    }

    @Test
    @Timeout(value = EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testDrainIsIdempotent()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createRunnerWithWorkers(2)) {
            TestingTrinoServer worker = getWorker(queryRunner);
            BufferNodeStateManager bufferNodeStateManager = worker.getInstance(Key.get(BufferNodeStateManager.class));
            ChunkManager chunkManager = worker.getInstance(Key.get(ChunkManager.class));
            DrainService drainService = worker.getInstance(Key.get(DrainService.class));

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE));

            drainService.drain();

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINING));

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));

            assertThat(chunkManager.getSpooledChunksCount())
                    .describedAs("No data should have been spooled when draining without active queries")
                    .isEqualTo(0);

            drainService.drain();
            assertThat(bufferNodeStateManager.getState())
                    .describedAs("drain() call after DRAINED must not change state")
                    .isEqualTo(BufferNodeState.DRAINED);
        }
    }

    @Test
    @Timeout(value = EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS, unit = MILLISECONDS)
    public void testDrainSpoolsChunks()
            throws Exception
    {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
        try (DistributedQueryRunner queryRunner = createRunnerWithWorkers(2)) {
            TestingTrinoServer worker = getWorker(queryRunner);
            BufferNodeStateManager bufferNodeStateManager = worker.getInstance(Key.get(BufferNodeStateManager.class));
            ChunkManager chunkManager = worker.getInstance(Key.get(ChunkManager.class));
            DrainService drainService = worker.getInstance(Key.get(DrainService.class));

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.ACTIVE));

            List<ListenableFuture<MaterializedResult>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(
                        () -> queryRunner.execute(
                                "SELECT COUNT(*), a.group_key " +
                                        "FROM mock.default.test_table a, mock.default.test_table b " +
                                        "GROUP BY a.group_key")));
            }

            assertEventually(new Duration(CHUNKS_AVAILABLE_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(chunkManager.getOpenChunks() + chunkManager.getClosedChunks())
                            .describedAs("Expected chunks to be present before triggering drain")
                            .isGreaterThan(0));

            drainService.drain();

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINING));

            assertEventually(new Duration(BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS, MILLISECONDS),
                    () -> assertThat(bufferNodeStateManager.getState()).isEqualTo(BufferNodeState.DRAINED));

            assertThat(chunkManager.getOpenChunks() + chunkManager.getClosedChunks())
                    .describedAs("All in-memory chunks should be gone after drain")
                    .isEqualTo(0);
            assertThat(chunkManager.getSpooledChunksCount())
                    .describedAs("Chunks should have been spooled to persistent storage during drain")
                    .isGreaterThan(0);

            Futures.allAsList(queryFutures).get();
        }
        finally {
            executor.shutdownNow();
        }
    }
}
