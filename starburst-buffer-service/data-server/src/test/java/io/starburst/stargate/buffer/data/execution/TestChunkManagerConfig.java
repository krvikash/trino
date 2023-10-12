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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.execution.ChunkManagerConfig.DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestChunkManagerConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(ChunkManagerConfig.class)
                .setChunkTargetSize(DataSize.of(16, MEGABYTE))
                .setChunkMaxSize(DataSize.of(64, MEGABYTE))
                .setChunkSliceSize(DataSize.of(128, KILOBYTE))
                .setExchangeStalenessThreshold(DEFAULT_EXCHANGE_STALENESS_THRESHOLD)
                .setSpoolingDirectory(null)
                .setChunkSpoolInterval(succinctDuration(50, MILLISECONDS))
                .setChunkSpoolConcurrency(32)
                .setChunkSpoolMergeEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("chunk.target-size", "32MB")
                .put("chunk.max-size", "128MB")
                .put("chunk.slice-size", "1MB")
                .put("exchange.staleness-threshold", "1m")
                .put("spooling.directory", "s3://spooling-bucket")
                .put("chunk.spool-interval", "5s")
                .put("chunk.spool-concurrency", "10")
                .put("chunk.spool-merge-enabled", "true")
                .buildOrThrow();

        ChunkManagerConfig expected = new ChunkManagerConfig()
                .setChunkTargetSize(DataSize.of(32, MEGABYTE))
                .setChunkMaxSize(DataSize.of(128, MEGABYTE))
                .setChunkSliceSize(DataSize.of(1, MEGABYTE))
                .setExchangeStalenessThreshold(succinctDuration(1, MINUTES))
                .setSpoolingDirectory("s3://spooling-bucket/")
                .setChunkSpoolInterval(succinctDuration(5, SECONDS))
                .setChunkSpoolConcurrency(10)
                .setChunkSpoolMergeEnabled(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidate()
    {
        ChunkManagerConfig config = new ChunkManagerConfig();
        config.setChunkSliceSize(DataSize.ofBytes(10_000));
        config.setChunkTargetSize(DataSize.ofBytes(10_000_000));
        config.setChunkMaxSize(DataSize.ofBytes(20_000_000));
        config.validate(); // ok

        config.setChunkTargetSize(DataSize.ofBytes(10_000_001));
        assertValidationFailed(config, "chunk.target-size must be a multiple of chunk.slice-size");

        config.setChunkTargetSize(DataSize.ofBytes(9_999_999));
        assertValidationFailed(config, "chunk.target-size must be a multiple of chunk.slice-size");

        config.setChunkTargetSize(DataSize.ofBytes(10_000_000));
        config.validate(); // ok

        config.setChunkMaxSize(DataSize.ofBytes(20_000_001));
        assertValidationFailed(config, "chunk.max-size must be a multiple of chunk.slice-size");

        config.setChunkMaxSize(DataSize.ofBytes(19_999_999));
        assertValidationFailed(config, "chunk.max-size must be a multiple of chunk.slice-size");

        config.setChunkMaxSize(DataSize.ofBytes(20_000_000));
        config.validate(); // ok

        config.setChunkTargetSize(DataSize.ofBytes(21_000_000));
        assertValidationFailed(config, "chunk.max-size must not be smaller than chunk.target-size");
    }

    private static void assertValidationFailed(ChunkManagerConfig config, String expectedMessage)
    {
        Assertions.assertThatThrownBy(config::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(expectedMessage);
    }
}
