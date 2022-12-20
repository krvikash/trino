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
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.SliceLease;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.ChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.murmur3_128;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.ErrorCode.CHUNK_NOT_FOUND;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getFileName;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectories;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.translateFailures;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3SpoolingStorage
        implements SpoolingStorage
{
    private final long bufferNodeId;
    private final MemoryAllocator memoryAllocator;
    private final List<String> bucketNames;
    private final S3AsyncClient s3AsyncClient;
    private final CounterStat spooledDataSize;
    private final CounterStat spoolingFailures;
    private final CounterStat unspooledDataSize;
    private final CounterStat unspoolingFailures;
    private final DistributionStat spooledChunkSizeDistribution;
    private final DistributionStat unspooledChunkSizeDistribution;
    private final ExecutorService executor;

    // exchangeId -> chunkId -> fileSize
    private final Map<String, Map<Long, Integer>> fileSizes = new ConcurrentHashMap<>();

    @Inject
    public S3SpoolingStorage(
            BufferNodeId bufferNodeId,
            MemoryAllocator memoryAllocator,
            ChunkManagerConfig chunkManagerConfig,
            SpoolingS3Config spoolingS3Config,
            DataServerStats dataServerStats,
            ExecutorService executor)
    {
        this.bufferNodeId = bufferNodeId.getLongValue();
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.bucketNames = requireNonNull(chunkManagerConfig.getSpoolingDirectories(), "spoolingDirectory is null").stream()
                .map(S3SpoolingStorage::getBucketName)
                .collect(toImmutableList());
        AwsCredentialsProvider credentialsProvider = createAwsCredentialsProvider(spoolingS3Config);
        RetryPolicy retryPolicy = RetryPolicy.builder(spoolingS3Config.getRetryMode())
                .numRetries(spoolingS3Config.getMaxErrorRetries())
                .build();
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(USER_AGENT_PREFIX, "")
                .putAdvancedOption(USER_AGENT_SUFFIX, "Trino-exchange")
                .build();
        S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfig)
                .region(spoolingS3Config.getRegion());
        spoolingS3Config.getS3Endpoint().ifPresent(s3Endpoint -> s3AsyncClientBuilder.endpointOverride(URI.create(s3Endpoint)));
        this.s3AsyncClient = s3AsyncClientBuilder.build();
        spooledDataSize = dataServerStats.getSpooledDataSize();
        spoolingFailures = dataServerStats.getSpoolingFailures();
        spooledChunkSizeDistribution = dataServerStats.getSpooledChunkSizeDistribution();
        unspooledDataSize = dataServerStats.getUnspooledDataSize();
        unspoolingFailures = dataServerStats.getUnspoolingFailures();
        unspooledChunkSizeDistribution = dataServerStats.getUnspooledChunkSizeDistribution();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ChunkDataLease readChunk(long bufferNodeId, String exchangeId, long chunkId)
    {
        String bucketName = selectBucket(bufferNodeId, exchangeId, chunkId);
        String fileName = getFileName(exchangeId, chunkId, bufferNodeId);
        Map<Long, Integer> chunkIdToFileSizes = fileSizes.get(exchangeId);
        int sliceLength;
        try {
            if (chunkIdToFileSizes != null && bufferNodeId == this.bufferNodeId) {
                Integer fileSize = chunkIdToFileSizes.get(chunkId);
                sliceLength = requireNonNullElseGet(fileSize, () -> getFileSize(bucketName, fileName));
            }
            else {
                // Synchronous communication to S3 for file size is rare and will only happen when a node dies
                // TODO: measure metrics to requesting file size from S3
                sliceLength = getFileSize(bucketName, fileName);
            }
        }
        catch (NoSuchKeyException e) {
            throw new DataServerException(CHUNK_NOT_FOUND,
                    "No closed chunk found for bufferNodeId %d, exchange %s, chunk %d".formatted(bufferNodeId, exchangeId, chunkId));
        }

        SliceLease sliceLease = new SliceLease(memoryAllocator, sliceLength);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(selectBucket(bufferNodeId, exchangeId, chunkId))
                .key(fileName)
                .build();
        return ChunkDataLease.forSliceLeaseAsync(
                sliceLease,
                slice -> translateFailures(toListenableFuture(s3AsyncClient.getObject(getObjectRequest, ChunkDataAsyncResponseTransformer.toChunkDataHolder(slice))
                        .whenComplete((holder, failure) -> {
                            if (failure == null) {
                                unspooledDataSize.update(sliceLength);
                                unspooledChunkSizeDistribution.add(sliceLength);
                            }
                            else {
                                unspoolingFailures.update(1);
                            }
                        }))),
                executor);
    }

    private String selectBucket(long bufferNodeId, String exchangeId, long chunkId)
    {
        if (bucketNames.size() == 1) {
            return bucketNames.get(0);
        }
        int bucketId = (abs(murmur3_128().newHasher()
                        .putLong(bufferNodeId)
                        .putString(exchangeId, UTF_8)
                        .putLong(chunkId)
                        .hash()
                        .asInt()) % bucketNames.size());
        return bucketNames.get(bucketId);
    }

    @Override
    public ListenableFuture<Void> writeChunk(long bufferNodeId, String exchangeId, long chunkId, ChunkDataHolder chunkDataHolder)
    {
        checkArgument(!chunkDataHolder.chunkSlices().isEmpty(), "unexpected empty chunk when spooling");

        String fileName = getFileName(exchangeId, chunkId, bufferNodeId);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(selectBucket(bufferNodeId, exchangeId, chunkId))
                .key(fileName)
                .build();
        fileSizes.computeIfAbsent(exchangeId, ignored -> new ConcurrentHashMap<>()).put(chunkId, chunkDataHolder.serializedSizeInBytes());
        CompletableFuture<PutObjectResponse> putObjectCompletableFuture = s3AsyncClient.putObject(putObjectRequest, ChunkDataAsyncRequestBody.fromChunkDataHolder(chunkDataHolder));
        // not chaining result with whenComplete as it breaks cancellation
        putObjectCompletableFuture.whenComplete((response, failure) -> {
            if (failure == null) {
                spooledDataSize.update(chunkDataHolder.serializedSizeInBytes());
                spooledChunkSizeDistribution.add(chunkDataHolder.serializedSizeInBytes());
            }
            else {
                spoolingFailures.update(1);
            }
        });
        return translateFailures(asVoid(toListenableFuture(putObjectCompletableFuture)));
    }

    @Override
    public ListenableFuture<Void> removeExchange(String exchangeId)
    {
        fileSizes.remove(exchangeId);
        ImmutableList.Builder<ListenableFuture<Void>> deleteDirectoryFutures = ImmutableList.builder();
        for (String bucketName : bucketNames) {
            for (String prefixedDirectory : getPrefixedDirectories(exchangeId)) {
                deleteDirectoryFutures.add(deleteDirectory(bucketName, prefixedDirectory));
            }
        }
        return translateFailures(asVoid(Futures.allAsList((deleteDirectoryFutures.build()))));
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

    private static AwsCredentialsProvider createAwsCredentialsProvider(SpoolingS3Config config)
    {
        String accessKey = config.getS3AwsAccessKey();
        String secretKey = config.getS3AwsSecretKey();

        if (accessKey != null && secretKey != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }
        if (accessKey == null && secretKey == null) {
            return DefaultCredentialsProvider.create();
        }
        throw new IllegalArgumentException("AWS access key and secret key should be either both set or both not set");
    }

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    private static String getBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    private int getFileSize(String bucketName, String fileName)
    {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        return toIntExact(getFutureValue(s3AsyncClient.headObject(headObjectRequest)).contentLength());
    }

    private ListObjectsV2Publisher listObjectsRecursively(String bucketName, String exchangeId)
    {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(exchangeId)
                .build();

        return s3AsyncClient.listObjectsV2Paginator(request);
    }

    public ListenableFuture<Void> deleteDirectory(String bucketName, String directoryName)
    {
        ImmutableList.Builder<String> keys = ImmutableList.builder();
        return asVoid(Futures.transformAsync(
                toListenableFuture((listObjectsRecursively(bucketName, directoryName)
                        .subscribe(listObjectsV2Response -> listObjectsV2Response.contents().stream()
                                .map(S3Object::key)
                                .forEach(keys::add)))),
                // deleteObjects has a limit of 1000
                ignored -> Futures.allAsList(Lists.partition(keys.build(), 1000).stream().map(list -> {
                    DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(list.stream().map(key -> ObjectIdentifier.builder().key(key).build()).collect(toImmutableList())).build())
                            .build();
                    return toListenableFuture(s3AsyncClient.deleteObjects(request));
                }).collect(toImmutableList())),
                directExecutor()));
    }
}
