/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.cloud;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.stargate.buffer.data.spooling.trinofs.TrinoFsSpoolingStorage;
import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;

import static java.util.Locale.ROOT;
import static java.util.UUID.randomUUID;

/**
 * Provisions a fresh Azure Blob container per test class and points {@link #rootUri} at it
 * so that {@link TrinoFsSpoolingStorage} writes/reads are isolated and cleanup is a single
 * container delete. Subclasses provide the account and {@link AzureAuth}.
 */
abstract class AbstractTestTrinoFsCloudSpoolingStorageOnAzure
        extends AbstractTestTrinoFsCloudSpoolingStorage
{
    enum AccountKind
    {
        FLAT, HIERARCHICAL
    }

    private AzureFileSystemFactory factory;
    private BlobContainerClient containerClient;

    protected abstract String account();

    protected abstract AzureAuth azureAuth();

    protected abstract AccountKind accountKind();

    @Override
    protected void setupCloudEnvironment()
    {
        String account = account();
        AzureAuth azureAuth = azureAuth();
        String containerName = "test-%s-%s".formatted(accountKind().name().toLowerCase(ROOT), randomUUID());

        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint("https://%s.blob.core.windows.net".formatted(account))
                .containerName(containerName);
        azureAuth.setAuth(account, builder);
        containerClient = builder.buildClient();
        containerClient.create();

        factory = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                azureAuth,
                new AzureFileSystemConfig());
        fileSystem = factory.create(ConnectorIdentity.ofUser("buffer"));
        rootUri = "abfs://%s@%s.dfs.core.windows.net/".formatted(containerName, account);
    }

    @Override
    protected void deleteRoot()
            throws IOException
    {
        // container delete in tearDownCloudEnvironment() wipes everything; nothing per-test to do
    }

    @Override
    protected void tearDownCloudEnvironment()
    {
        try {
            if (containerClient != null) {
                containerClient.deleteIfExists();
            }
        }
        finally {
            containerClient = null;
            if (factory != null) {
                try {
                    factory.destroy();
                }
                finally {
                    factory = null;
                }
            }
        }
    }
}
