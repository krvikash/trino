/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class TestLocalDiskTierConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(LocalDiskTierConfig.class)
                .setDirectory(null)
                .setCapacity(null)
                .setMemorySkipThreshold(null)
                .setSpoolingHighWatermark(0.8)
                .setSpoolingLowWatermark(0.5));
    }

    @Test
    public void testExplicitPropertyMappings(@TempDir Path directory)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("local-disk.directory", directory.toString())
                .put("local-disk.capacity", "100GB")
                .put("local-disk.memory-skip-threshold", "8GB")
                .put("local-disk.spooling-high-watermark", "0.9")
                .put("local-disk.spooling-low-watermark", "0.6")
                .buildOrThrow();

        LocalDiskTierConfig expected = new LocalDiskTierConfig()
                .setDirectory(directory)
                .setCapacity(DataSize.of(100, GIGABYTE))
                .setMemorySkipThreshold(DataSize.of(8, GIGABYTE))
                .setSpoolingHighWatermark(0.9)
                .setSpoolingLowWatermark(0.6);

        assertFullMapping(properties, expected);
    }
}
