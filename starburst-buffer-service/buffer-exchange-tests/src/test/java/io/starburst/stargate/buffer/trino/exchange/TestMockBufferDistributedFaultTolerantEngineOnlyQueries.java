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

import static io.airlift.testing.Closeables.closeAllSuppress;

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

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
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
}
