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
import io.starburst.stargate.buffer.trino.exchange.BufferExchangePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.tpch.TpchTable;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public final class TestingBufferServiceExchangeMemoryRunner
{
    private TestingBufferServiceExchangeMemoryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        TestingBufferService bufferService = TestingBufferService
                .builder()
                .withDiscoveryServerBuilder(
                        builder -> builder.setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                .build();
        URI discoveryServerUri = bufferService.getDiscoveryServer().getBaseUri();

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", discoveryServerUri.toString())
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .buildOrThrow();

        Map<String, String> properties = new HashMap<>();
        properties.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        properties.put("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(properties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new BufferExchangePlugin());
                    runner.loadExchangeManager("buffer", exchangeManagerProperties);
                })
                .setInitialTables(TpchTable.getTables())
                .build();
    }
}
