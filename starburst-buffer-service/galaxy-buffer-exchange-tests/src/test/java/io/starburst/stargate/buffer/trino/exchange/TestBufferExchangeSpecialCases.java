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
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.testing.MockBufferExchangePlugin;
import io.starburst.stargate.buffer.testing.MockBufferService;
import io.starburst.stargate.buffer.testing.MockDataNodeStats;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.testing.assertions.Assert;
import org.assertj.core.api.AssertProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.airlift.testing.Closeables.closeAll;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.FAILED_GET_CHUNK_DATA_CHUNK_DRAINED_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.FAILED_GET_CHUNK_DATA_NOT_FOUND_IN_DRAINED_STORAGE_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT;
import static io.starburst.stargate.buffer.testing.MockDataNodeStats.Key.SUCCESSFUL_GET_CHUNK_DATA_FROM_DRAINED_STORAGE_REQUEST_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBufferExchangeSpecialCases
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testDrainingWhileLongRunningQuery()
            throws Exception
    {
        try (TestSetup testSetup = new TestSetup()) {
            MockBufferService mockBufferService = testSetup.getMockBufferService();

            // long-running query which and writes reasonably sized output (not too small, not too big) to an exchange.
            Future<AssertProvider<QueryAssertions.QueryAssert>> queryFuture = executor.submit(() -> testSetup.query("""
                    WITH big        AS (SELECT custkey k FROM tpch.sf10.orders),
                         small AS (SELECT custkey AS k FROM tpch.tiny.customer WHERE acctbal < 10)
                    SELECT count(*) FROM small,big WHERE small.k = big.k"""));

            int nodeToBeDrained = 1;
            Assert.assertEventually(Duration.valueOf("2m"), () -> {
                // wait until some data is written to node to be drained
                MockDataNodeStats stats = mockBufferService.getNodeStats(nodeToBeDrained);
                assertThat(stats.get(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT)).isGreaterThan(0);
            });
            long addedNode = mockBufferService.addNode();
            // mark nodes as draining; it will still respond to getChunkData with 200
            mockBufferService.markNodeDraining(nodeToBeDrained);

            queryFuture.get();
            assertThat(queryFuture.get()).matches("VALUES (BIGINT '1480')");
            MockDataNodeStats drainedNodeStats = mockBufferService.getNodeStats(nodeToBeDrained);
            MockDataNodeStats newNodeStats = mockBufferService.getNodeStats(addedNode);

            assertThat(drainedNodeStats.get(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT)).isGreaterThan(0);
            assertThat(newNodeStats.get(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT)).isGreaterThan(0);

            // all data is served from node it was written to (no drained storage access)
            mockBufferService.getAllNodeIds().forEach(nodeId -> {
                assertThat(mockBufferService.getNodeStats(nodeId).get(FAILED_GET_CHUNK_DATA_NOT_FOUND_IN_DRAINED_STORAGE_REQUEST_COUNT)).isEqualTo(0);
                assertThat(mockBufferService.getNodeStats(nodeId).get(FAILED_GET_CHUNK_DATA_CHUNK_DRAINED_REQUEST_COUNT)).isEqualTo(0);
            });
        }
    }

    @Test
    @Disabled // TODO(https://github.com/starburstdata/galaxy-trino/issues/1570) reenable after fixing flakiness
    public void testNodeFullyDrainedWhileLongRunningQuery()
            throws Exception
    {
        try (TestSetup testSetup = new TestSetup()) {
            MockBufferService mockBufferService = testSetup.getMockBufferService();

            // long-running query which and writes reasonably sized output (not too small, not too big) to an exchange.
            Future<AssertProvider<QueryAssertions.QueryAssert>> queryFuture = executor.submit(() -> testSetup.query("""
                    WITH big        AS (SELECT custkey k FROM tpch.sf10.orders),
                         single_row AS (SELECT custkey AS k FROM tpch.tiny.customer WHERE acctbal = 2237.64)
                    SELECT count(*) FROM single_row,big WHERE single_row.k = big.k"""));

            int nodeToBeDrained = 1;
            Assert.assertEventually(Duration.valueOf("1m"), () -> {
                // wait until some data is written to node to be drained
                MockDataNodeStats stats = mockBufferService.getNodeStats(nodeToBeDrained);
                assertThat(stats.get(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT)).isGreaterThan(0);
            });
            long addedNode = mockBufferService.addNode();
            // mark nodes as drained; it will flush data to persistent storage and return 404 for getChunkData requests
            mockBufferService.markNodeDrained(nodeToBeDrained);

            queryFuture.get();
            assertThat(queryFuture.get()).matches("VALUES (BIGINT '25')");
            MockDataNodeStats drainedNodeStats = mockBufferService.getNodeStats(nodeToBeDrained);
            MockDataNodeStats newNodeStats = mockBufferService.getNodeStats(addedNode);

            assertThat(drainedNodeStats.get(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT)).isGreaterThan(0);
            assertThat(newNodeStats.get(SUCCESSFUL_ADD_DATA_PAGES_REQUEST_COUNT)).isGreaterThan(0);

            // drained node rejected on some getChunkData requests
            assertThat(drainedNodeStats.get(FAILED_GET_CHUNK_DATA_CHUNK_DRAINED_REQUEST_COUNT)).isGreaterThan(0);
            // some data have been served from drained storage
            long dataServedFromDrainedStorage = 0;
            for (Long nodeId : mockBufferService.getAllNodeIds()) {
                dataServedFromDrainedStorage += mockBufferService.getNodeStats(nodeId).get(SUCCESSFUL_GET_CHUNK_DATA_FROM_DRAINED_STORAGE_REQUEST_COUNT);
            }
            assertThat(dataServedFromDrainedStorage).isGreaterThan(0);
        }
    }

    private static class TestSetup
            implements Closeable
    {
        private final QueryRunner queryRunner;
        private final MockBufferService mockBufferService;
        private final QueryAssertions assertions;

        public TestSetup()
                throws Exception
        {
            mockBufferService = new MockBufferService(3);

            ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("exchange.buffer-discovery.uri", "http://dummy") // required
                    .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                    // setup sink to write to be super aggressive writing to buffer nodes
                    // to lower chance data is not written within timeout.
                    .put("exchange.sink-min-written-pages-size", "0B")
                    .put("exchange.sink-min-written-pages-count", "0")
                    .put("exchange.sink-writer-max-wait", "0s")
                    .buildOrThrow();

            Map<String, String> extraProperties = new HashMap<>();
            extraProperties.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
            extraProperties.put("fault-tolerant-execution-min-partition-count", "50"); // use more partition to be sure we get some on the drained node
            extraProperties.put("fault-tolerant-execution-max-partition-count", "50");
            extraProperties.put("optimizer.join-reordering-strategy", "NONE");
            extraProperties.put("join-distribution-type", "PARTITIONED");
            extraProperties.put("enable-dynamic-filtering", "false");

            queryRunner = MemoryQueryRunner.builder()
                    .setExtraProperties(extraProperties)
                    .setAdditionalSetup(runner -> {
                        runner.installPlugin(new MockBufferExchangePlugin(mockBufferService));
                        runner.loadExchangeManager("buffer", exchangeManagerProperties);
                    })
                    .setInitialTables(Set.of())
                    .build();

            assertions = new QueryAssertions(queryRunner);
        }

        public AssertProvider<QueryAssertions.QueryAssert> query(String query)
        {
            return assertions.query(query);
        }

        public MockBufferService getMockBufferService()
        {
            return mockBufferService;
        }

        @Override
        public void close()
                throws IOException
        {
            closeAll(
                    assertions,
                    queryRunner);
        }
    }

    @AfterAll
    public void destroy()
    {
        executor.shutdownNow();
    }
}
