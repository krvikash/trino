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
import io.trino.tpch.TpchTable;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SplitTestingBufferServiceSetup
{
    public static class BufferServiceMain
    {
        private static final int DATA_SERVERS_COUNT = 1;

        private BufferServiceMain() {}

        public static void main(String[] args)
        {
            TestingBufferService bufferService = TestingBufferService
                    .builder()
                    .setDataServersCount(DATA_SERVERS_COUNT)
                    .withDiscoveryServerBuilder(
                            builder -> builder
                                    .setConfigProperty("http-server.http.port", "9090")
                                    .setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                    .withDataServerBuilder(builder -> builder
                            .setConfigProperty("http-server.http.port", "9091"))
                    .build();

            URI discoveryServerUri = bufferService.getDiscoveryServer().getBaseUri();
        }
    }

    public static class TrinoQueryRunnerMain
    {
        private TrinoQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Map<String, String> properties = new HashMap<>();
            properties.put("retry-policy", "TASK");
            properties.put("retry-initial-delay", "50ms");
            properties.put("retry-max-delay", "100ms");
            properties.put("fault-tolerant-execution-partition-count", "5");
            properties.put("fault-tolerant-execution-target-task-input-size", "10MB");
            properties.put("fault-tolerant-execution-target-task-split-count", "4");
                    // to trigger spilling
            properties.put("exchange.deduplication-buffer-size", "1kB");
            properties.put("fault-tolerant-execution-task-memory", "1GB");
                    // limit number of threads to detect potential thread leaks
            properties.put("query.executor-pool-size", "10");
                    // enable exchange compression to follow production deployment recommendations
            properties.put("exchange.compression-enabled", "true");
            properties.put("http-server.http.port", "8080");

            ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("exchange.buffer-discovery.uri", "http://localhost:9090")
                    .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                    .put("exchange.source-blocked-memory-high", "64MB")
                    .put("exchange.source-blocked-memory-low", "32MB")
                    .buildOrThrow();

            DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                    .setExtraProperties(properties)
                    .setNodeCount(1)
                    .setAdditionalSetup(runner -> {
                        runner.installPlugin(new BufferExchangePlugin());
                        runner.loadExchangeManager("buffer", exchangeManagerProperties);
                    })
                    .setInitialTables(TpchTable.getTables())
                    .build();
        }
    }
}
