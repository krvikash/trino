/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestAzureBlobSpoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AzureBlobSpoolingConfig.class)
                .setUploadBlockSize(DataSize.of(4, MEGABYTE))
                .setUploadMaxConcurrency(4));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.azure.upload-block-size", "3MB")
                .put("spooling.azure.upload-max-concurrency", "1")
                .buildOrThrow();

        AzureBlobSpoolingConfig expected = new AzureBlobSpoolingConfig()
                .setUploadBlockSize(DataSize.of(3, MEGABYTE))
                .setUploadMaxConcurrency(1);

        assertFullMapping(properties, expected);
    }
}
