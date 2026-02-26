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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSpoolingDirectoryConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(SpoolingDirectoryConfig.class)
                .setSpoolingDirectory(null)
                .setAllowLocalSpooling(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.directory", "s3://spooling-bucket")
                .put("testing.allow-local-spooling", "true")
                .buildOrThrow();

        SpoolingDirectoryConfig expected = new SpoolingDirectoryConfig()
                .setSpoolingDirectory("s3://spooling-bucket/")
                .setAllowLocalSpooling(true);

        assertFullMapping(properties, expected);
    }
}
