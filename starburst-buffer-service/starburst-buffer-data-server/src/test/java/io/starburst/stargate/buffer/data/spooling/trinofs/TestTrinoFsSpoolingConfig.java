/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.trinofs;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestTrinoFsSpoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TrinoFsSpoolingConfig.class)
                .setExecutorThreads(50)
                .setDeleteExecutorThreads(10));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.trino-fs.executor-threads", "16")
                .put("spooling.trino-fs.delete-executor-threads", "4")
                .buildOrThrow();

        TrinoFsSpoolingConfig expected = new TrinoFsSpoolingConfig()
                .setExecutorThreads(16)
                .setDeleteExecutorThreads(4);

        assertFullMapping(properties, expected);
    }
}
