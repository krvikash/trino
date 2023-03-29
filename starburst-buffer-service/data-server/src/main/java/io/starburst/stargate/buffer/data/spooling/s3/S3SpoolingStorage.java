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
import software.amazon.awssdk.services.s3.S3AsyncClient;
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
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.getBucketName;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class S3SpoolingStorage
        extends AbstractSpoolingStorage
{
    private final String bucketName;
    private final S3AsyncClient s3AsyncClient;

    @Inject
    public S3SpoolingStorage(
            BufferNodeId bufferNodeId,
            ChunkManagerConfig chunkManagerConfig,
            S3AsyncClient s3AsyncClient,
            DataServerStats dataServerStats)
    {
        super(bufferNodeId, dataServerStats);

        this.s3AsyncClient = s3AsyncClient;
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
