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

import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountAuth;
import io.trino.filesystem.gcs.GcsServiceAccountAuthConfig;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;

public class TestTrinoFsCloudSpoolingStorageOnGcs
        extends AbstractTestTrinoFsCloudSpoolingStorage
{
    private GcsFileSystemFactory factory;

    @Override
    protected void setupCloudEnvironment()
    {
        String bucket = requireEnv("GCP_STORAGE_BUCKET");
        byte[] jsonKeyBytes = Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY"));

        GcsFileSystemConfig config = new GcsFileSystemConfig();
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig()
                .setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsServiceAccountAuth auth;
        try {
            auth = new GcsServiceAccountAuth(authConfig);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, auth);
        factory = new GcsFileSystemFactory(config, storageFactory);
        fileSystem = factory.create(ConnectorIdentity.ofUser("buffer"));
        rootUri = "gs://%s/trino-fs-spool-test-%s/".formatted(bucket, randomUUID());
    }

    @Override
    protected void tearDownCloudEnvironment()
    {
        if (factory != null) {
            try {
                factory.stop();
            }
            finally {
                factory = null;
            }
        }
    }
}
