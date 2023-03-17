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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.AbstractSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpooledChunkNotFoundException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.endpoint.DefaultServiceEndpointBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.createAwsCredentialsProvider;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.getBucketName;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3SpoolingStorage
        extends AbstractSpoolingStorage
{
    private final String bucketName;
    private final S3AsyncClient s3AsyncClient;

    @Inject
    public S3SpoolingStorage(
            BufferNodeId bufferNodeId,
            ChunkManagerConfig chunkManagerConfig,
            SpoolingS3Config spoolingS3Config,
            DataServerStats dataServerStats)
    {
        super(bufferNodeId, dataServerStats);
        AwsCredentialsProvider credentialsProvider = createAwsCredentialsProvider(
                spoolingS3Config.getS3AwsAccessKey(),
                spoolingS3Config.getS3AwsSecretKey());
        RetryPolicy retryPolicy = RetryPolicy.builder(spoolingS3Config.getRetryMode())
                .numRetries(spoolingS3Config.getMaxErrorRetries())
                .build();
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(USER_AGENT_PREFIX, "")
                .putAdvancedOption(USER_AGENT_SUFFIX, "Trino-exchange")
                .build();

        Optional<Region> region = spoolingS3Config.getRegion();
        Optional<String> endpoint = spoolingS3Config.getS3Endpoint();

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
        this.bucketName = getBucketName(requireNonNull(chunkManagerConfig.getSpoolingDirectory(), "spoolingDirectory is null"));
    }

    @Override
    protected ListenableFuture<?> putStorageObject(String fileName, ChunkDataLease chunkDataLease)
    {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        return toListenableFuture(s3AsyncClient.putObject(putObjectRequest, ChunkDataAsyncRequestBody.fromChunkDataLease(chunkDataLease)));
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(s3AsyncClient::close);
        }
    }

    protected int getFileSize(String fileName) throws SpooledChunkNotFoundException
    {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        try {
            return toIntExact(getFutureValue(s3AsyncClient.headObject(headObjectRequest)).contentLength());
        }
        catch (NoSuchKeyException e) {
            throw new SpooledChunkNotFoundException(e);
        }
    }

    @Override
    protected String getLocation(String fileName)
    {
        return "s3://" + bucketName + PATH_SEPARATOR + fileName;
    }

    @Override
    protected ListenableFuture<Void> deleteDirectories(List<String> directoryNames)
    {
        ImmutableList.Builder<ListenableFuture<List<String>>> listObjectsFuturesBuilder = ImmutableList.builder();
        for (String directoryName : directoryNames) {
            ImmutableList.Builder<String> keys = ImmutableList.builder();
            ListenableFuture<List<String>> listObjectsFuture = Futures.transform(
                    toListenableFuture((listObjectsRecursively(directoryName)
                            .subscribe(listObjectsV2Response -> listObjectsV2Response.contents().stream()
                                    .map(S3Object::key)
                                    .forEach(keys::add)))),
                    ignored -> keys.build(),
                    directExecutor());
            listObjectsFuturesBuilder.add(listObjectsFuture);
        }
        return asVoid(Futures.transformAsync(
                Futures.allAsList(listObjectsFuturesBuilder.build()),
                nestedList -> Futures.allAsList(Lists.partition(nestedList.stream().flatMap(Collection::stream).collect(toImmutableList()), 1000).stream().map(list -> {
                    DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(list.stream().map(key -> ObjectIdentifier.builder().key(key).build()).collect(toImmutableList())).build())
                            .build();
                    return toListenableFuture(s3AsyncClient.deleteObjects(request));
                }).collect(toImmutableList())),
                directExecutor()));
    }

    private ListObjectsV2Publisher listObjectsRecursively(String exchangeId)
    {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(exchangeId)
                .build();

        return s3AsyncClient.listObjectsV2Paginator(request);
    }
}
