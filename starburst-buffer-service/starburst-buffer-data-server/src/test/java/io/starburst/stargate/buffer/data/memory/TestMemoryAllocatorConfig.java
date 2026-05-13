/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.memory;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestMemoryAllocatorConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(MemoryAllocatorConfig.class)
                .setSpoolingRatioLowWatermark(0.75)
                .setSpoolingRatioHighWatermark(0.9)
                .setChunkSlicePoolingFraction(0.8));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("memory.spooling-low-watermark", "0.5")
                .put("memory.spooling-high-watermark", "0.99")
                .put("memory.chunk-slice-pool-fraction", "0.66")
                .buildOrThrow();

        MemoryAllocatorConfig expected = new MemoryAllocatorConfig()
                .setSpoolingRatioLowWatermark(0.5)
                .setSpoolingRatioHighWatermark(0.99)
                .setChunkSlicePoolingFraction(0.66);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyPropertyMappings()
    {
        Map<String, String> currentProperties = ImmutableMap.<String, String>builder()
                .put("memory.spooling-low-watermark", "0.5")
                .put("memory.spooling-high-watermark", "0.99")
                .buildOrThrow();
        Map<String, String> legacyProperties = ImmutableMap.<String, String>builder()
                .put("memory.allocation-low-watermark", "0.5")
                .put("memory.allocation-high-watermark", "0.99")
                .buildOrThrow();

        assertDeprecatedEquivalence(MemoryAllocatorConfig.class, currentProperties, legacyProperties);
    }
}
