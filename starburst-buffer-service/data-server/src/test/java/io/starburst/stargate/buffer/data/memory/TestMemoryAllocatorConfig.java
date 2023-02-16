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
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class TestMemoryAllocatorConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(MemoryAllocatorConfig.class)
                .setHeapHeadroom(DataSize.ofBytes(Math.round(Runtime.getRuntime().maxMemory() * 0.2)))
                .setAllocationRatioLowWatermark(0.75)
                .setAllocationRatioHighWatermark(0.9)
                .setChunkSlicePoolingFraction(0.8));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("memory.heap-headroom", "2GB")
                .put("memory.allocation-low-watermark", "0.5")
                .put("memory.allocation-high-watermark", "0.99")
                .put("memory.chunk-slice-pool-fraction", "0.66")
                .buildOrThrow();

        MemoryAllocatorConfig expected = new MemoryAllocatorConfig()
                .setHeapHeadroom(DataSize.of(2, GIGABYTE))
                .setAllocationRatioLowWatermark(0.5)
                .setAllocationRatioHighWatermark(0.99)
                .setChunkSlicePoolingFraction(0.66);

        assertFullMapping(properties, expected);
    }
}
