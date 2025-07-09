/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.retry.RetryMode;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestS3ClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(S3ClientConfig.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setRegion(null)
                .setS3Endpoint(null)
                .setRetryMode(RetryMode.ADAPTIVE)
                .setMaxErrorRetries(10));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("spooling.s3.aws-access-key", "access")
                .put("spooling.s3.aws-secret-key", "secret")
                .put("spooling.s3.region", "us-east-1")
                .put("spooling.s3.endpoint", "endpoint")
                .put("spooling.s3.retry-mode", "STANDARD")
                .put("spooling.s3.max-error-retries", "8")
                .buildOrThrow();

        S3ClientConfig expected = new S3ClientConfig()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret")
                .setRegion("us-east-1")
                .setS3Endpoint("endpoint")
                .setRetryMode(RetryMode.STANDARD)
                .setMaxErrorRetries(8);

        assertFullMapping(properties, expected);
    }
}
