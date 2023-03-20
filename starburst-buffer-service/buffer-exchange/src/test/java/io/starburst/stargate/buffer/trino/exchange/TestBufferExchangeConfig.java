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
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.trino.exchange.PartitionNodeMappingMode.PINNING_MULTI;
import static io.starburst.stargate.buffer.trino.exchange.PartitionNodeMappingMode.PINNING_SINGLE;
import static java.util.concurrent.TimeUnit.SECONDS;

class TestBufferExchangeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BufferExchangeConfig.class)
                .setDiscoveryServiceUri(null)
                .setSinkBlockedMemoryLowWaterMark(DataSize.of(128, MEGABYTE))
                .setSinkBlockedMemoryHighWaterMark(DataSize.of(256, MEGABYTE))
                .setSourceBlockedMemoryLowWaterMark(DataSize.of(32, MEGABYTE))
                .setSourceBlockedMemoryHighWaterMark(DataSize.of(64, MEGABYTE))
                .setSourceParallelism(16)
                .setSourceHandleTargetChunksCount(64)
                .setSourceHandleTargetDataSize(DataSize.of(256, MEGABYTE))
                .setSinkTargetWrittenPagesCount(512)
                .setSinkTargetWrittenPagesSize(DataSize.of(16, MEGABYTE))
                .setSinkTargetWrittenPartitionsCount(16)
                .setPartitionNodeMappingMode(PINNING_MULTI)
                .setMinBufferNodesPerPartition(2)
                .setMaxBufferNodesPerPartition(32)
                .setDataClientMaxRetries(5)
                .setDataClientRetryBackoffInitial(succinctDuration(2.0, SECONDS))
                .setDataClientRetryBackoffMax(succinctDuration(60.0, SECONDS))
                .setDataClientRetryBackoffFactor(2.0)
                .setDataClientRetryBackoffJitter(0.5)
                .setDataClientCircuitBreakerFailureThreshold(10)
                .setDataClientCircuitBreakerSuccessThreshold(5)
                .setDataClientCircuitBreakerDelay(succinctDuration(30.0, SECONDS))
                .setDataClientAddDataPagesMaxRetries(5)
                .setDataClientAddDataPagesRetryBackoffInitial(succinctDuration(16.0, SECONDS))
                .setDataClientAddDataPagesRetryBackoffMax(succinctDuration(120.0, SECONDS))
                .setDataClientAddDataPagesRetryBackoffFactor(2.0)
                .setDataClientAddDataPagesRetryBackoffJitter(0.5)
                .setDataClientAddDataPagesCircuitBreakerFailureThreshold(10)
                .setDataClientAddDataPagesCircuitBreakerSuccessThreshold(5)
                .setDataClientAddDataPagesCircuitBreakerDelay(succinctDuration(60.0, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws URISyntaxException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", "http://some-discovery-host:123")
                .put("exchange.sink-blocked-memory-low", "256MB")
                .put("exchange.sink-blocked-memory-high", "1024MB")
                .put("exchange.source-blocked-memory-low", "257MB")
                .put("exchange.source-blocked-memory-high", "1025MB")
                .put("exchange.source-parallelism", "5")
                .put("exchange.source-handle-target-chunks-count", "128")
                .put("exchange.source-handle-target-data-size", "1GB")
                .put("exchange.sink-target-written-pages-count", "5")
                .put("exchange.sink-target-written-pages-size", "7MB")
                .put("exchange.sink-target-written-partitions-count", "9")
                .put("exchange.partition-node-mapping-mode", "PINNING_SINGLE")
                .put("exchange.min-buffer-nodes-per-partition", "3")
                .put("exchange.max-buffer-nodes-per-partition", "33")
                .put("exchange.buffer-data.max-retries", "6")
                .put("exchange.buffer-data.retry-backoff-initial", "3s")
                .put("exchange.buffer-data.retry-backoff-max", "20s")
                .put("exchange.buffer-data.retry-backoff-factor", "4.0")
                .put("exchange.buffer-data.retry-backoff-jitter", "0.25")
                .put("exchange.buffer-data.circuit-breaker-failure-threshold", "11")
                .put("exchange.buffer-data.circuit-breaker-success-threshold", "6")
                .put("exchange.buffer-data.circuit-breaker-delay", "31s")
                .put("exchange.buffer-data.add-data-pages-max-retries", "6")
                .put("exchange.buffer-data.add-data-pages-retry-backoff-initial", "3s")
                .put("exchange.buffer-data.add-data-pages-retry-backoff-max", "20s")
                .put("exchange.buffer-data.add-data-pages-retry-backoff-factor", "4.0")
                .put("exchange.buffer-data.add-data-pages-retry-backoff-jitter", "0.25")
                .put("exchange.buffer-data.add-data-pages-circuit-breaker-failure-threshold", "11")
                .put("exchange.buffer-data.add-data-pages-circuit-breaker-success-threshold", "6")
                .put("exchange.buffer-data.add-data-pages-circuit-breaker-delay", "31s")
                .buildOrThrow();

        BufferExchangeConfig expected = new BufferExchangeConfig()
                .setDiscoveryServiceUri(new URI("http://some-discovery-host:123"))
                .setSinkBlockedMemoryLowWaterMark(DataSize.of(256, MEGABYTE))
                .setSinkBlockedMemoryHighWaterMark(DataSize.of(1024, MEGABYTE))
                .setSourceBlockedMemoryLowWaterMark(DataSize.of(257, MEGABYTE))
                .setSourceBlockedMemoryHighWaterMark(DataSize.of(1025, MEGABYTE))
                .setSourceParallelism(5)
                .setSourceHandleTargetChunksCount(128)
                .setSourceHandleTargetDataSize(DataSize.of(1, GIGABYTE))
                .setSinkTargetWrittenPagesCount(5)
                .setSinkTargetWrittenPagesSize(DataSize.of(7, MEGABYTE))
                .setSinkTargetWrittenPartitionsCount(9)
                .setPartitionNodeMappingMode(PINNING_SINGLE)
                .setMinBufferNodesPerPartition(3)
                .setMaxBufferNodesPerPartition(33)
                .setDataClientMaxRetries(6)
                .setDataClientRetryBackoffInitial(succinctDuration(3.0, SECONDS))
                .setDataClientRetryBackoffMax(succinctDuration(20.0, SECONDS))
                .setDataClientRetryBackoffFactor(4.0)
                .setDataClientRetryBackoffJitter(0.25)
                .setDataClientCircuitBreakerFailureThreshold(11)
                .setDataClientCircuitBreakerSuccessThreshold(6)
                .setDataClientCircuitBreakerDelay(succinctDuration(31, SECONDS))
                .setDataClientAddDataPagesMaxRetries(6)
                .setDataClientAddDataPagesRetryBackoffInitial(succinctDuration(3.0, SECONDS))
                .setDataClientAddDataPagesRetryBackoffMax(succinctDuration(20.0, SECONDS))
                .setDataClientAddDataPagesRetryBackoffFactor(4.0)
                .setDataClientAddDataPagesRetryBackoffJitter(0.25)
                .setDataClientAddDataPagesCircuitBreakerFailureThreshold(11)
                .setDataClientAddDataPagesCircuitBreakerSuccessThreshold(6)
                .setDataClientAddDataPagesCircuitBreakerDelay(succinctDuration(31, SECONDS));

        assertFullMapping(properties, expected);
    }
}
