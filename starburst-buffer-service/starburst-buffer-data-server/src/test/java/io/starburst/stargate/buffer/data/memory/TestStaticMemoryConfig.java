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

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestStaticMemoryConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(StaticMemoryConfig.class)
                .setBaseMemory("10%")
                .setChunksMemory(String.valueOf((long) (HeapSizeParser.DEFAULT.parse("10%").toBytes() * 0.8)) + "B"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("memory.base", "2GB")
                .put("memory.chunks", "1GB")
                .buildOrThrow();

        StaticMemoryConfig expected = new StaticMemoryConfig()
                .setBaseMemory("2GB")
                .setChunksMemory("1GB");

        assertFullMapping(properties, expected);
    }
}
