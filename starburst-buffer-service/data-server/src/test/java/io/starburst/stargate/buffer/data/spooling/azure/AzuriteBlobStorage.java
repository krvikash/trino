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
    private final GenericContainer<?> container;

    public AzuriteBlobStorage()
    {
        this.container = new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite")
                .withExposedPorts(10000)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "azurite-blob",
                        "--blobHost",
                        "0.0.0.0"));
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

    public String getConnectionString()
    {
        // This is the default connection string defined by the azurite container
        return "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:" + container.getMappedPort(10000) + "/devstoreaccount1;";
    }
}
