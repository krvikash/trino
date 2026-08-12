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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.file.Files.createTempDirectory;

final class EmbeddedBufferQueryRunner
{
    static final long EMBEDDED_BUFFER_TEST_TIMEOUT_MILLIS = 240_000;
    static final long BUFFER_NODE_STATE_TRANSITION_TIMEOUT_MILLIS = 30_000;
    static final long CHUNKS_AVAILABLE_TIMEOUT_MILLIS = 30_000;

    private EmbeddedBufferQueryRunner() {}

    static TestingTrinoServer getWorker(DistributedQueryRunner queryRunner)
    {
        return queryRunner.getServers()
                .stream()
                .filter(server -> !server.isCoordinator())
                .findFirst()
                .orElseThrow();
    }

    static DistributedQueryRunner createSingleNodeRunner()
            throws Exception
    {
        return createRunnerWithWorkers(0);
    }

    static DistributedQueryRunner createRunnerWithWorkers(int workerCount)
            throws Exception
    {
        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        File exchangeManagerDirectory = createTempDirectory("exchange_manager").toFile();
        extraProperties.put("embedded-buffer-service-enabled", "true");
        extraProperties.put("buffer.spooling.directory", exchangeManagerDirectory.getAbsolutePath());
        extraProperties.put("buffer.testing.allow-local-spooling", "true");
        extraProperties.put("buffer.draining.min-duration", "5s");
        extraProperties.put("query.max-memory-per-node", "30%");
        extraProperties.put("query.executor-pool-size", "10");
        extraProperties.put("shutdown.grace-period", "1s");

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.use-embedded-buffer-service", "true")
                .put("exchange.sink-target-written-pages-count", "3")
                .put("exchange.source-handle-target-chunks-count", "4")
                .put("exchange.min-base-buffer-nodes-per-partition", "1")
                .put("exchange.max-base-buffer-nodes-per-partition", "1")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .addCoordinatorProperty("node-scheduler.include-coordinator", "true")
                .setExtraProperties(extraProperties)
                .setWorkerCount(workerCount)
                .withExchange("buffer", exchangeManagerProperties)
                .build();

        installMockConnector(queryRunner);

        return queryRunner;
    }

    static void installMockConnector(DistributedQueryRunner queryRunner)
    {
        queryRunner.installPlugin(new MockConnectorPlugin(
                MockConnectorFactory.builder()
                        .withGetColumns(_ -> ImmutableList.of(
                                new ColumnMetadata("id", BIGINT),
                                new ColumnMetadata("group_key", VARCHAR)))
                        .withData(_ -> {
                            ImmutableList.Builder<List<?>> rows = ImmutableList.builder();
                            for (int i = 0; i < 5000; i++) {
                                rows.add(ImmutableList.of((long) i, "group_" + (i % 100)));
                            }
                            return rows.build();
                        })
                        .build()));
        queryRunner.createCatalog("mock", "mock");
    }
}
