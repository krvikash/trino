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

import com.azure.storage.common.policy.RetryPolicyType;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestAzureBlobClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AzureBlobClientConfig.class)
                .setConnectionString(null)
                .setRetryPolicyType(null)
                .setMaxTries(null)
                .setTryTimeout(new Duration(10, SECONDS))
                .setRetryDelay(null)
                .setMaxRetryDelay(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.azure.connection-string", "connection")
                .put("spooling.azure.retry-policy", "FIXED")
                .put("spooling.azure.max-tries", "3")
                .put("spooling.azure.try-timeout", "4s")
                .put("spooling.azure.retry-delay", "5ms")
                .put("spooling.azure.max-retry-delay", "6ms")
                .buildOrThrow();

        AzureBlobClientConfig expected = new AzureBlobClientConfig()
                .setConnectionString("connection")
                .setRetryPolicyType(RetryPolicyType.FIXED)
                .setMaxTries(3)
                .setTryTimeout(new Duration(4, SECONDS))
                .setRetryDelay(new Duration(5, MILLISECONDS))
                .setMaxRetryDelay(new Duration(6, MILLISECONDS));

        assertFullMapping(properties, expected);
    }
}
