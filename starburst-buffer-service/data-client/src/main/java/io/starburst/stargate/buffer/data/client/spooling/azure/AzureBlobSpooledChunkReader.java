/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.azure;

import com.azure.core.http.rest.ResponseBase;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobRange;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static io.starburst.stargate.buffer.data.spooling.azure.AzureSpoolUtils.getContainerName;
import static io.starburst.stargate.buffer.data.spooling.azure.AzureSpoolUtils.keyFromUri;
import static java.util.Objects.requireNonNull;

public class AzureBlobSpooledChunkReader
        implements SpooledChunkReader
{
    private final BlobServiceAsyncClient azureClient;
    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public AzureBlobSpooledChunkReader(
            DataApiConfig dataApiConfig,
            BlobServiceAsyncClient blobServiceAsyncClient)
    {
        this.dataIntegrityVerificationEnabled = requireNonNull(dataApiConfig, "dataApiConfig is null").isDataIntegrityVerificationEnabled();
        this.azureClient = requireNonNull(blobServiceAsyncClient, "blobServiceAsyncClient is null");
    }

    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
    {
        URI spooledChunkUri = URI.create(spooledChunk.location());
        long offset = spooledChunk.offset();
        int length = spooledChunk.length();

        String scheme = spooledChunkUri.getScheme();
        checkArgument(spooledChunkUri.getScheme().equals("abfs"), "Unexpected storage scheme '%s' for AzureSpooledChunkReader, expecting 'abfs'", scheme);

        return toListenableFuture(azureClient.getBlobContainerAsyncClient(getContainerName(spooledChunkUri))
                .getBlobAsyncClient(keyFromUri(spooledChunkUri))
                .downloadStreamWithResponse(new BlobRange(offset, (long) length), null, null, false)
                .flatMapMany(ResponseBase::getValue)
                .reduce(ByteBuffer.allocate(length), ByteBuffer::put)
                .map(byteBuffer -> toDataPages(byteBuffer.array(), dataIntegrityVerificationEnabled))
                .toFuture());
    }

    @Override
    public void close() {}
}
