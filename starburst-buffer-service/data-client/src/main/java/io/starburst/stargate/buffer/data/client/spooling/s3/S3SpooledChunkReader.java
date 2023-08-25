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
import com.google.inject.Inject;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.getBucketName;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.keyFromUri;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static java.util.Objects.requireNonNull;

public class S3SpooledChunkReader
        implements SpooledChunkReader
{
    private final S3AsyncClient s3AsyncClient;
    private final ExecutorService executor;
    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public S3SpooledChunkReader(
            S3AsyncClient s3AsyncClient,
            DataApiConfig dataApiConfig,
            ExecutorService executor)
    {
        this.s3AsyncClient = s3AsyncClient;
        this.dataIntegrityVerificationEnabled = dataApiConfig.isDataIntegrityVerificationEnabled();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
    {
        URI uri = URI.create(spooledChunk.location());
        long offset = spooledChunk.offset();
        int length = spooledChunk.length();

        String scheme = uri.getScheme();
        checkArgument(scheme.equals("s3") || scheme.equals("gs"), "Unexpected storage scheme %s for S3SpooledChunkReader, expecting s3/gs", scheme);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(getBucketName(uri))
                .key(keyFromUri(uri))
                .range("bytes=" + offset + "-" + (offset + length - 1))
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
