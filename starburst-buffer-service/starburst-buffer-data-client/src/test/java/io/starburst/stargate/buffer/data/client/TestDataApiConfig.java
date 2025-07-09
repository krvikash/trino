/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestDataApiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataApiConfig.class)
                .setDataIntegrityVerificationEnabled(true)
                .setSpoolingStorageType(SpoolingStorageType.NONE));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("data-integrity-verification-enabled", "false")
                .put("spooling-storage-type", "s3")
                .buildOrThrow();

        DataApiConfig expected = new DataApiConfig()
                .setDataIntegrityVerificationEnabled(false)
                .setSpoolingStorageType(SpoolingStorageType.S3);

        assertFullMapping(properties, expected);
    }
}
