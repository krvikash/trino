/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAiFileStorageConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AiFileStorageConfig.class)
                .setModelConnectionSpecsFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ai.client.models.file", "test-models.json")
                .buildOrThrow();
        AiFileStorageConfig expected = new AiFileStorageConfig()
                .setModelConnectionSpecsFile("test-models.json");

        assertFullMapping(properties, expected);
    }
}
