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
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestTrinoFsSpoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TrinoFsSpoolingConfig.class)
                .setExecutorThreads(128)
                .setDeleteExecutorThreads(10)
                .setOperationTimeout(new Duration(60, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.trino-fs.executor-threads", "16")
                .put("spooling.trino-fs.delete-executor-threads", "4")
                .put("spooling.trino-fs.operation-timeout", "2m")
                .buildOrThrow();

        TrinoFsSpoolingConfig expected = new TrinoFsSpoolingConfig()
                .setExecutorThreads(16)
                .setDeleteExecutorThreads(4)
                .setOperationTimeout(new Duration(2, MINUTES));

        assertFullMapping(properties, expected);
    }
}
