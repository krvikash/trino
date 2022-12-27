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
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingStorageType;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

class TestDataApiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataApiConfig.class)
                .setDataIntegrityVerificationEnabled(true)
                .setDataClientMaxRetries(5)
                .setDataClientRetryBackoffInitial(Duration.succinctDuration(1.0, SECONDS))
                .setDataClientRetryBackoffMax(Duration.succinctDuration(10.0, SECONDS))
                .setDataClientRetryBackoffFactor(2.0)
                .setDataClientRetryBackoffJitter(0.5)
                .setSpoolingStorageType(SpoolingStorageType.NONE));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws URISyntaxException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("data-integrity-verification-enabled", "false")
                .put("max-retries", "6")
                .put("retry-backoff-initial", "3s")
                .put("retry-backoff-max", "20s")
                .put("retry-backoff-factor", "4.0")
                .put("retry-backoff-jitter", "0.25")
                .put("spooling-storage-type", "s3")
                .buildOrThrow();

        DataApiConfig expected = new DataApiConfig()
                .setDataIntegrityVerificationEnabled(false)
                .setDataClientMaxRetries(6)
                .setDataClientRetryBackoffInitial(Duration.succinctDuration(3.0, SECONDS))
                .setDataClientRetryBackoffMax(Duration.succinctDuration(20.0, SECONDS))
                .setDataClientRetryBackoffFactor(4.0)
                .setDataClientRetryBackoffJitter(0.25)
                .setSpoolingStorageType(SpoolingStorageType.S3);

        assertFullMapping(properties, expected);
    }
}
