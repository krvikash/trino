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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;

public class AzuriteBlobStorage
        implements Startable
{
    public static final String ACCOUNT = "devstoreaccount1";
    // Well-known Azurite account key (https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite)
    public static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final GenericContainer<?> container;

    public AzuriteBlobStorage()
    {
        this.container = new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite")
                .withExposedPorts(10000)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "azurite-blob",
                        "--blobHost",
                        "0.0.0.0",
                        "--skipApiVersionCheck")); // TODO Remove this option once "The API version 2026-02-06 is not supported by Azurite. Please upgrade Azurite to latest version and retry" error is fixed
    }

    @Override
    public void start()
    {
        container.start();
    }

    @Override
    public void stop()
    {
        container.stop();
    }

    public String getBlobEndpoint()
    {
        return "http://127.0.0.1:" + container.getMappedPort(10000) + "/" + ACCOUNT;
    }

    public String getConnectionString()
    {
        // This is the default connection string defined by the azurite container
        return "DefaultEndpointsProtocol=http;AccountName=" + ACCOUNT + ";AccountKey=" + ACCOUNT_KEY + ";BlobEndpoint=" + getBlobEndpoint() + ";";
    }
}
