/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestChunkManagerConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(ChunkManagerConfig.class)
                .setChunkMaxSize(DataSize.of(16, MEGABYTE))
                .setChunkSliceSize(DataSize.of(128, KILOBYTE))
                .setExchangeStalenessThreshold(succinctDuration(5, MINUTES))
                .setSpoolingDirectory(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("chunk.max-size", "32MB")
                .put("chunk.slice-size", "1MB")
                .put("exchange.staleness-threshold", "1m")
                .put("spooling.directory", "s3://spooling-bucket")
                .buildOrThrow();

        ChunkManagerConfig expected = new ChunkManagerConfig()
                .setChunkMaxSize(DataSize.of(32, MEGABYTE))
                .setChunkSliceSize(DataSize.of(1, MEGABYTE))
                .setExchangeStalenessThreshold(succinctDuration(1, MINUTES))
                .setSpoolingDirectory("s3://spooling-bucket/");

        assertFullMapping(properties, expected);
    }
}
