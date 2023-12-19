/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createAzureBlobSpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createAzureBlobSpoolingStorage;

public class TestAzureBlobSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    private AzuriteBlobStorage azuriteBlobStorage;
    private BlobServiceAsyncClient blobServiceAsyncClient;

    @Override
    @BeforeAll
    public void init()
    {
        azuriteBlobStorage = new AzuriteBlobStorage();
        azuriteBlobStorage.start();
        super.init();
    }

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        blobServiceAsyncClient = new BlobServiceClientBuilder()
                .connectionString(azuriteBlobStorage.getConnectionString())
                .buildAsyncClient();
        String containerName = "spooling-storage-" + UUID.randomUUID();

        blobServiceAsyncClient.createBlobContainer(containerName).block();

        return createAzureBlobSpoolingStorage(blobServiceAsyncClient, containerName);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createAzureBlobSpooledChunkReader(blobServiceAsyncClient);
    }

    @Override
    @AfterAll
    public void destroy()
            throws Exception
    {
        super.destroy();
        if (azuriteBlobStorage != null) {
            azuriteBlobStorage.stop();
        }
    }
}
