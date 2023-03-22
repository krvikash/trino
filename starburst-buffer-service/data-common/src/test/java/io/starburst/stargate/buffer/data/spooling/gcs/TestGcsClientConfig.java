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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGcsClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsClientConfig.class)
                .setGcsEndpoint(null)
                .setGcsJsonKey(null)
                .setGcsJsonKeyFilePath(null)
                .setGcsRetryMode(GcsRetryMode.DEFAULT)
                .setThreadCount(50));
    }

    @Test
    public void testExplicitPropertyMapping()
            throws IOException
    {
        Path jsonKeyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.gcs.endpoint", "endpoint")
                .put("spooling.gcs.json-key", "foo")
                .put("spooling.gcs.json-key-file-path", jsonKeyFile.toString())
                .put("spooling.gcs.retry-mode", "UNIFORM")
                .put("spooling.gcs.thread-count", "100")
                .buildOrThrow();

        GcsClientConfig expected = new GcsClientConfig()
                .setGcsEndpoint("endpoint")
                .setGcsJsonKey("foo")
                .setGcsJsonKeyFilePath(jsonKeyFile.toString())
                .setGcsRetryMode(GcsRetryMode.UNIFORM)
                .setThreadCount(100);

        assertFullMapping(properties, expected);
    }
}
