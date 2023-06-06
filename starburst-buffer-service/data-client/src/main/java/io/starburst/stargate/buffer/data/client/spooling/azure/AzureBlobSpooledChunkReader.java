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

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;

import javax.inject.Inject;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.getBucketName;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.keyFromUri;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
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
    public ListenableFuture<List<DataPage>> getDataPages(SpoolingFile spoolingFile)
    {
        URI spoolingFileUri = URI.create(spoolingFile.location());

        String scheme = spoolingFileUri.getScheme();
        checkArgument(spoolingFileUri.getScheme().equals("ms"), "Unexpected storage scheme '%s' for AzureSpooledChunkReader, expecting 'ms'", scheme);

        return toListenableFuture(azureClient.getBlobContainerAsyncClient(getBucketName(spoolingFileUri))
                .getBlobAsyncClient(keyFromUri(spoolingFileUri))
                .download()
                .reduce(ByteBuffer.allocate(spoolingFile.length()), ByteBuffer::put)
                .map(byteBuffer -> toDataPages(byteBuffer.array(), dataIntegrityVerificationEnabled))
                .toFuture());
    }

    @Override
    public void close() {}
}
