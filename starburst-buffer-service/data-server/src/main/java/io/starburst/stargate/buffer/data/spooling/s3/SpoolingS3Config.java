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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.Locale.ENGLISH;

public class SpoolingS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private Region region;
    private Optional<String> s3Endpoint = Optional.empty();
    private RetryMode retryMode = RetryMode.ADAPTIVE;
    private int maxErrorRetries = 10;

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("spooling.s3.aws-access-key")
    public SpoolingS3Config setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("spooling.s3.aws-secret-key")
    @ConfigSecuritySensitive
    public SpoolingS3Config setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public Region getRegion()
    {
        return region;
    }

    @Config("spooling.s3.region")
    public SpoolingS3Config setRegion(String region)
    {
        if (region != null) {
            this.region = Region.of(region.toLowerCase(ENGLISH));
        }
        return this;
    }

    public Optional<String> getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("spooling.s3.endpoint")
    public SpoolingS3Config setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = Optional.ofNullable(s3Endpoint);
        return this;
    }

    @NotNull
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @Config("spooling.s3.retry-mode")
    public SpoolingS3Config setRetryMode(RetryMode retryMode)
    {
        this.retryMode = retryMode;
        return this;
    }

    @Min(1)
    public int getMaxErrorRetries()
    {
        return maxErrorRetries;
    }

    @Config("spooling.s3.max-error-retries")
    public SpoolingS3Config setMaxErrorRetries(int maxErrorRetries)
    {
        this.maxErrorRetries = maxErrorRetries;
        return this;
    }
}
