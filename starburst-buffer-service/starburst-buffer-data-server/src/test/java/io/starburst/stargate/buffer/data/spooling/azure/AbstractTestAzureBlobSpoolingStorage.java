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
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static java.util.UUID.randomUUID;

public abstract class AbstractTestAzureBlobSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    protected AzuriteBlobStorage azuriteBlobStorage;
    protected BlobServiceAsyncClient blobServiceAsyncClient;
    protected String containerName;

    @Override
    @BeforeAll
    public void init()
    {
        azuriteBlobStorage = new AzuriteBlobStorage();
        azuriteBlobStorage.start();
        blobServiceAsyncClient = new BlobServiceClientBuilder()
                .connectionString(azuriteBlobStorage.getConnectionString())
                .buildAsyncClient();
        containerName = "spooling-storage-" + randomUUID();
        blobServiceAsyncClient.createBlobContainer(containerName).block();
        super.init();
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
