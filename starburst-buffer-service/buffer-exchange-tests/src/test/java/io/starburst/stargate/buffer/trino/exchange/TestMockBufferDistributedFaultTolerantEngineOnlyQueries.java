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
import io.starburst.stargate.buffer.testing.MockBufferExchangePlugin;
import io.starburst.stargate.buffer.testing.MockBufferService;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMockBufferDistributedFaultTolerantEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MockBufferService mockBufferService = new MockBufferService(2);

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", "http://dummy") // required
                .put("exchange.sink-target-written-pages-count", "3") // small requests for better test coverage
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .put("exchange.min-buffer-nodes-per-partition", "2")
                .put("exchange.max-buffer-nodes-per-partition", "2")
                .buildOrThrow();

        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        // By default, FaultTolerantExecutionConnectorTestHelper.getExtraProperties sets
        // executor-pool-size to 10. Such small value may cause queries to fail if tests are run in parallel.
        // The reason for that is currently same thread pool is used for long running jobs driving EventDrivenFaultTolerantQueryScheduler
        // as well as for future callback used during query processing. If all threads are used by EventDrivenFaultTolerantQueryScheduler jobs
        // then callbacks would not be executed and queries may get blocked.
        // TODO: update code in Trino so it is not needed
        extraProperties.put("query.executor-pool-size", "100");
        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new MockBufferExchangePlugin(mockBufferService));
                    runner.loadExchangeManager("buffer", exchangeManagerProperties);
                })
                .setInitialTables(TpchTable.getTables())
                .build();

        queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        try {
            queryRunner.installPlugin(new MockConnectorPlugin(
                    MockConnectorFactory.builder()
                            .withSessionProperties(TEST_CATALOG_PROPERTIES)
                            .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Override
    @Test
    @Disabled
    public void testExplainAnalyzeVerbose()
    {
        // Spooling exchange does not prove output buffer utilization histogram
    }

    @Override
    @Test
    @Disabled
    public void testSelectiveLimit()
    {
        // FTE mode does not terminate query when limit is reached
    }

    @Override
    @Test
    public void testNonDeterministicAggregationPredicatePushdown()
    {
        // disabled in 434 as trinodb/trino does not have a fix and this test with buffer service will hang
        // remove override after update to 435
        try {
            Properties dependenciesVersions = new Properties();
            try (InputStream inputStream = getResource(getClass(), "/io/starburst/stargate/buffer/trino/exchange/dependencies-versions.properties").openStream()) {
                dependenciesVersions.load(inputStream);
            }

            String trinoDependencyVersion = dependenciesVersions.getProperty("dep.trino.version");

            assertThat(trinoDependencyVersion).as("trino version").isEqualTo("434");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Test
    public void testRowNumberLimit()
    {
        // disabled in 434 as trinodb/trino does not have a fix and this test with buffer service will hang
        // remove override after update to 435
        try {
            Properties dependenciesVersions = new Properties();
            try (InputStream inputStream = getResource(getClass(), "/io/starburst/stargate/buffer/trino/exchange/dependencies-versions.properties").openStream()) {
                dependenciesVersions.load(inputStream);
            }

            String trinoDependencyVersion = dependenciesVersions.getProperty("dep.trino.version");

            assertThat(trinoDependencyVersion).as("trino version").isEqualTo("434");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
