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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.trino.faulttolerant.iceberg.BaseIcebergFailureRecoveryTest;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class TestTestingBufferIcebergQueryFailureRecoveryTest
        extends BaseIcebergFailureRecoveryTest
{
    protected TestTestingBufferIcebergQueryFailureRecoveryTest()
    {
        super(RetryPolicy.TASK);
    }

    private static final Logger log = Logger.get(TestTestingBufferDistributedFaultTolerantEngineOnlyQueries.class);

    private TestingBufferService bufferService;

    @Override
    protected QueryRunner createQueryRunner(List<TpchTable<?>> requiredTpchTables, Map<String, String> configProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long memoryHeadroom = (long) (0.7 * maxMemory);
        int dataServersCount = 3;
        log.info("Starting testing buffer service with %d data nodes; each with %s memory", dataServersCount, DataSize.succinctBytes(maxMemory - memoryHeadroom));

        bufferService = TestingBufferService
                .builder()
                .withDiscoveryServerBuilder(
                        builder -> builder.setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                .withDataServerBuilder(
                        builder -> builder
                                .setConfigProperty("memory.heap-headroom", DataSize.succinctBytes(memoryHeadroom).toString())
                                .setConfigProperty("exchange.staleness-threshold", "2h")) //tmp
                .setDataServersCount(dataServersCount)
                .build();
        URI discoveryServerUri = bufferService.getDiscoveryServer().getBaseUri();

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", discoveryServerUri.toString())
                .buildOrThrow();

        return IcebergQueryRunner.builder()
                .setInitialTables(requiredTpchTables)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(configProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new BufferExchangePlugin());
                    runner.loadExchangeManager("buffer", exchangeManagerProperties);
                })
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (bufferService != null) {
            bufferService.close();
            bufferService = null;
        }
    }
}
