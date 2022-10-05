/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

public final class MockBufferServiceExchangeMemoryRunner
{
    private MockBufferServiceExchangeMemoryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        MockBufferService mockBufferService = new MockBufferService(1);
        Map<String, String> properties = new HashMap<>();
        properties.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        properties.put("http-server.http.port", "8080");
        properties.put("task.max-partial-aggregation-memory", "0kB");
        properties.put("adaptive-partial-aggregation.enabled", "false");

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(properties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new MockBufferExchangePlugin(mockBufferService));
                    runner.loadExchangeManager(
                            "buffer",
                            ImmutableMap.<String, String>builder()
                                    .put("exchange.buffer-discovery.uri", "http://dummy") // required
                                    .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                                    .buildOrThrow());
                })
                .setInitialTables(TpchTable.getTables())
                .build();
    }
}
