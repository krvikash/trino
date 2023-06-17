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

import jakarta.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.awscore.endpoint.DefaultServiceEndpointBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.Optional;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3Utils
{
    public static AwsCredentialsProvider createAwsCredentialsProvider(@Nullable String accessKey, @Nullable String secretKey)
    {
        return createAwsCredentialsProvider(Optional.ofNullable(accessKey), Optional.ofNullable(secretKey));
    }

    public static AwsCredentialsProvider createAwsCredentialsProvider(Optional<String> accessKey, Optional<String> secretKey)
    {
        if (accessKey.isPresent() && secretKey.isPresent()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey.get(), secretKey.get()));
        }
        if (accessKey.isEmpty() && secretKey.isEmpty()) {
            return WebIdentityTokenFileCredentialsProvider.create();
        }
        throw new IllegalArgumentException("AWS access key and secret key should be either both set or both not set");
    }

    public static S3AsyncClient createS3Client(S3ClientConfig s3ClientConfig)
    {
        AwsCredentialsProvider credentialsProvider = createAwsCredentialsProvider(
                s3ClientConfig.getS3AwsAccessKey(),
                s3ClientConfig.getS3AwsSecretKey());
        RetryPolicy retryPolicy = RetryPolicy.builder(s3ClientConfig.getRetryMode())
                .numRetries(s3ClientConfig.getMaxErrorRetries())
                .build();
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(USER_AGENT_PREFIX, "")
                .putAdvancedOption(USER_AGENT_SUFFIX, "Trino-exchange")
                .build();

        Optional<Region> region = s3ClientConfig.getRegion();
        Optional<String> endpoint = s3ClientConfig.getS3Endpoint();

        if (endpoint.isPresent() && region.isPresent()) {
            throw new IllegalArgumentException("Either S3 endpoint or region can be specified");
        }

        S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .serviceConfiguration(S3Configuration.builder()
                        .checksumValidationEnabled(false)
                        .build())
                .overrideConfiguration(overrideConfig);

        s3AsyncClientBuilder.endpointOverride(
                endpoint.map(URI::create)
                        .orElseGet(() -> {
                            DefaultServiceEndpointBuilder endPointBuilder = new DefaultServiceEndpointBuilder("s3", "http");
                            region.ifPresent(endPointBuilder::withRegion);
                            return endPointBuilder.getServiceEndpoint();
                        }));

        region.ifPresent(s3AsyncClientBuilder::region);

        s3AsyncClientBuilder.forcePathStyle(true);

        return s3AsyncClientBuilder.build();
    }

    private S3Utils()
    {
    }
}
