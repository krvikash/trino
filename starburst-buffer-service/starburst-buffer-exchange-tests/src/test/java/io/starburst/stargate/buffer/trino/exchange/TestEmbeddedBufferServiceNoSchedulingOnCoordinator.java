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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;

public class TestEmbeddedBufferServiceNoSchedulingOnCoordinator
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        Path exchangeManagerPath = createTempDirectory("exchange_manager");
        closeAfterClass(() -> deleteRecursively(exchangeManagerPath, ALLOW_INSECURE));
        File exchangeManagerDirectory = exchangeManagerPath.toFile();
        extraProperties.put("embedded-buffer-service-enabled", "true");
        extraProperties.put("buffer.spooling.directory", exchangeManagerDirectory.getAbsolutePath());
        extraProperties.put("buffer.testing.allow-local-spooling", "true");
        extraProperties.put("query.max-memory-per-node", "30%");
        extraProperties.put("query.executor-pool-size", "100");

        // exchange manager config
        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.use-embedded-buffer-service", "true")
                .put("exchange.sink-target-written-pages-count", "3") // small requests for better test coverage
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .put("exchange.min-base-buffer-nodes-per-partition", "2")
                .put("exchange.max-base-buffer-nodes-per-partition", "2")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                .setExtraProperties(extraProperties)
                .withExchange("buffer", exchangeManagerProperties)
                .build();
        return queryRunner;
    }

    @Test
    public void testEmbeddedBufferServiceNoSchedulingOnCoordinator()
    {
        assertQuery("select count(*) from tpch.sf1.nation", "select 25");
    }
}
