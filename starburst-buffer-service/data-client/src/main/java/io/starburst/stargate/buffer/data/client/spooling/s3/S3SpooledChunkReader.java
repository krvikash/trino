/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.s3;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.endpoint.DefaultServiceEndpointBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.createAwsCredentialsProvider;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.getBucketName;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.keyFromUri;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3SpooledChunkReader
        implements SpooledChunkReader
{
    private final S3AsyncClient s3AsyncClient;
    private final ExecutorService executor;
    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public S3SpooledChunkReader(
            SpoolingS3ReaderConfig spoolingS3ReaderConfig,
            DataApiConfig dataApiConfig,
            ExecutorService executor)
    {
        AwsCredentialsProvider credentialsProvider = createAwsCredentialsProvider(
                spoolingS3ReaderConfig.getS3AwsAccessKey(),
                spoolingS3ReaderConfig.getS3AwsSecretKey());
        RetryPolicy retryPolicy = RetryPolicy.builder(spoolingS3ReaderConfig.getRetryMode())
                .numRetries(spoolingS3ReaderConfig.getMaxErrorRetries())
                .build();
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(USER_AGENT_PREFIX, "")
                .putAdvancedOption(USER_AGENT_SUFFIX, "Trino-exchange")
                .build();

        Optional<String> endpoint = spoolingS3ReaderConfig.getS3Endpoint();
        Optional<Region> region = spoolingS3ReaderConfig.getRegion();

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

        this.s3AsyncClient = s3AsyncClientBuilder.build();
        this.dataIntegrityVerificationEnabled = dataApiConfig.isDataIntegrityVerificationEnabled();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpoolingFile spoolingFile)
    {
        URI uri = URI.create(spoolingFile.location());
        int length = spoolingFile.length();

        String scheme = uri.getScheme();
        checkArgument(uri.getScheme().equals("s3"), "Unexpected storage scheme %s for S3SpooledChunkReader, expecting s3", scheme);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(getBucketName(uri))
                .key(keyFromUri(uri))
                .build();
        return Futures.transform(
                toListenableFuture(s3AsyncClient.getObject(getObjectRequest, ByteArrayAsyncResponseTransformer.toByteArray(length))),
                bytes -> toDataPages(bytes, dataIntegrityVerificationEnabled),
                executor);
    }

    @PreDestroy
    @Override
    public void close()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            closer.register(s3AsyncClient::close);
        }
    }
}
