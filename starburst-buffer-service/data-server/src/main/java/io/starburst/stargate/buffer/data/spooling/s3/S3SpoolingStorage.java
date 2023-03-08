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
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
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
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.createAwsCredentialsProvider;
import static io.starburst.stargate.buffer.data.client.spooling.s3.S3SpoolUtils.getBucketName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.translateFailures;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3SpoolingStorage
        implements SpoolingStorage
{
    private final long bufferNodeId;
    private final String bucketName;
    private final S3AsyncClient s3AsyncClient;
    private final CounterStat spooledDataSize;
    private final CounterStat spoolingFailures;
    private final DistributionStat spooledChunkSizeDistribution;

    // exchangeId -> chunkId -> fileSize
    private final Map<String, Map<Long, Integer>> fileSizes = new ConcurrentHashMap<>();

    @Inject
    public S3SpoolingStorage(
            BufferNodeId bufferNodeId,
            ChunkManagerConfig chunkManagerConfig,
            SpoolingS3Config spoolingS3Config,
            DataServerStats dataServerStats)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.bucketName = getBucketName(requireNonNull(chunkManagerConfig.getSpoolingDirectory(), "spoolingDirectory is null"));
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
        spooledDataSize = dataServerStats.getSpooledDataSize();
        spoolingFailures = dataServerStats.getSpoolingFailures();
        spooledChunkSizeDistribution = dataServerStats.getSpooledChunkSizeDistribution();
    }

    @Override
    public SpoolingFile getSpoolingFile(long bufferNodeId, String exchangeId, long chunkId)
    {
        String fileName = getFileName(bufferNodeId, exchangeId, chunkId);
        Map<Long, Integer> chunkIdToFileSizes = fileSizes.get(exchangeId);
        int length;
        try {
            if (chunkIdToFileSizes != null && bufferNodeId == this.bufferNodeId) {
                Integer fileSize = chunkIdToFileSizes.get(chunkId);
                length = requireNonNullElseGet(fileSize, () -> getFileSize(fileName));
            }
            else {
                // Synchronous communication to S3 for file size is rare and will only happen when a node dies
                // TODO: measure metrics to requesting file size from S3
                length = getFileSize(fileName);
            }
        }
        catch (NoSuchKeyException e) {
            throw new DataServerException(CHUNK_NOT_FOUND,
                    "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(bufferNodeId, exchangeId, chunkId));
        }
        return new SpoolingFile(getLocation(bucketName, fileName), length);
    }

    @Override
    public ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataLease chunkDataLease)
    {
        checkArgument(!chunkDataLease.chunkSlices().isEmpty(), "unexpected empty chunk when spooling");

        String fileName = getFileName(bufferNodeId, exchangeId, chunkId);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        fileSizes.computeIfAbsent(exchangeId, ignored -> new ConcurrentHashMap<>()).put(chunkId, chunkDataLease.serializedSizeInBytes());
        CompletableFuture<PutObjectResponse> putObjectCompletableFuture = s3AsyncClient.putObject(putObjectRequest, ChunkDataAsyncRequestBody.fromChunkDataLease(chunkDataLease));
        // not chaining result with whenComplete as it breaks cancellation
        putObjectCompletableFuture.whenComplete((response, failure) -> {
            if (failure == null) {
                spooledDataSize.update(chunkDataLease.serializedSizeInBytes());
                spooledChunkSizeDistribution.add(chunkDataLease.serializedSizeInBytes());
            }
            else {
                spoolingFailures.update(1);
            }
        });
        return translateFailures(asVoid(toListenableFuture(putObjectCompletableFuture)));
    }

    @Override
    public ListenableFuture<Void> removeExchange(long bufferNodeId, String exchangeId)
    {
        fileSizes.remove(exchangeId);
        return translateFailures(deleteDirectories(getPrefixedDirectories(bufferNodeId, exchangeId)));
    }

    @Override
    public int getSpooledChunks()
    {
        return fileSizes.values().stream().mapToInt(Map::size).sum();
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

    private int getFileSize(String fileName)
    {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        return toIntExact(getFutureValue(s3AsyncClient.headObject(headObjectRequest)).contentLength());
    }

    private String getLocation(String bucketName, String fileName)
    {
        return "s3://" + bucketName + PATH_SEPARATOR + fileName;
    }

    private ListenableFuture<Void> deleteDirectories(List<String> directoryNames)
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
