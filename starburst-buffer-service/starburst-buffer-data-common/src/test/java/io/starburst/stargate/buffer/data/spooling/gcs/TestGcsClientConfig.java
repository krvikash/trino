/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.gcs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestGcsClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsClientConfig.class)
                .setGcsJsonKey(null)
                .setGcsJsonKeyFilePath(null)
                .setDeleteExecutorThreadCount(50));
    }

    @Test
    public void testExplicitPropertyMappingsJsonKey()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.gcs.json-key", "key")
                .put("spooling.gcs.delete-executor-thread-count", "100")
                .buildOrThrow();

        GcsClientConfig expected = new GcsClientConfig()
                .setGcsJsonKey("key")
                .setDeleteExecutorThreadCount(100);

        assertFullMapping(properties, expected, ImmutableSet.of("spooling.gcs.json-key-file-path"));
    }

    @Test
    public void testExplicitPropertyMappingsJsonKeyFilePath()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.gcs.json-key-file-path", "/dev/null")
                .put("spooling.gcs.delete-executor-thread-count", "100")
                .buildOrThrow();

        GcsClientConfig expected = new GcsClientConfig()
                .setGcsJsonKeyFilePath("/dev/null")
                .setDeleteExecutorThreadCount(100);

        assertFullMapping(properties, expected, ImmutableSet.of("spooling.gcs.json-key"));
    }

    @Test
    public void testValidation()
    {
        assertFailsValidation(
                new GcsClientConfig()
                        .setGcsJsonKey("key")
                        .setGcsJsonKeyFilePath("/dev/null"),
                "jsonKeyConfigValid",
                "spooling.gcs.json-key and spooling.gcs.json-key-file-path are mutually exclusive",
                AssertTrue.class);
    }
}
