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

import javax.ws.rs.HEAD;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.trino.exchange.PartitionNodeMappingMode.PINNING;
import static io.starburst.stargate.buffer.trino.exchange.PartitionNodeMappingMode.RANDOM;

class TestBufferExchangeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BufferExchangeConfig.class)
                .setDiscoveryServiceUri(null)
                .setSinkBlockedMemoryLowWaterMark(DataSize.of(128, MEGABYTE))
                .setSinkBlockedMemoryHighWaterMark(DataSize.of(512, MEGABYTE))
                .setSourceBlockedMemoryLowWaterMark(DataSize.of(128, MEGABYTE))
                .setSourceBlockedMemoryHighWaterMark(DataSize.of(512, MEGABYTE))
                .setSourceParallelism(4)
                .setDataIntegrityVerificationEnabled(true)
                .setSourceHandleTargetChunksCount(64)
                .setSourceHandleTargetDataSize(DataSize.of(256, MEGABYTE))
                .setEncryptionEnabled(true)
                .setSinkTargetWrittenPagesCount(32)
                .setSinkTargetWrittenPagesSize(DataSize.of(8, MEGABYTE))
                .setPartitionNodeMappingMode(PINNING));
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
                .put("exchange.data-integrity-verification-enabled", "false")
                .put("exchange.source-handle-target-chunks-count", "128")
                .put("exchange.source-handle-target-data-size", "1GB")
                .put("exchange.encryption-enabled", "false")
                .put("exchange.sink-target-written-pages-count", "5")
                .put("exchange.sink-target-written-pages-size", "7MB")
                .put("exchange.partition-node-mapping-mode", "RANDOM")
                .buildOrThrow();

        BufferExchangeConfig expected = new BufferExchangeConfig()
                .setDiscoveryServiceUri(new URI("http://some-discovery-host:123"))
                .setSinkBlockedMemoryLowWaterMark(DataSize.of(256, MEGABYTE))
                .setSinkBlockedMemoryHighWaterMark(DataSize.of(1024, MEGABYTE))
                .setSourceBlockedMemoryLowWaterMark(DataSize.of(257, MEGABYTE))
                .setSourceBlockedMemoryHighWaterMark(DataSize.of(1025, MEGABYTE))
                .setSourceParallelism(5)
                .setDataIntegrityVerificationEnabled(false)
                .setSourceHandleTargetChunksCount(128)
                .setSourceHandleTargetDataSize(DataSize.of(1, GIGABYTE))
                .setEncryptionEnabled(false)
                .setSinkTargetWrittenPagesCount(5)
                .setSinkTargetWrittenPagesSize(DataSize.of(7, MEGABYTE))
                .setPartitionNodeMappingMode(RANDOM);

        assertFullMapping(properties, expected);
    }
}
