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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingClientDriver.NATIVE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingClientDriver.TRINO_FS;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.NONE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType.S3;

class TestDataApiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataApiConfig.class)
                .setDataIntegrityVerificationEnabled(true)
                .setSpoolingStorageType(NONE)
                .setSpoolingClientDriver(NATIVE));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("data-integrity-verification-enabled", "false")
                .put("spooling-storage-type", "s3")
                .put("spooling-client-driver", "TRINO_FS")
                .buildOrThrow();

        DataApiConfig expected = new DataApiConfig()
                .setDataIntegrityVerificationEnabled(false)
                .setSpoolingStorageType(S3)
                .setSpoolingClientDriver(TRINO_FS);

        assertFullMapping(properties, expected);
    }
}
