/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.azure.storage.blob.BlobServiceAsyncClient;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.azure.AzureBlobSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.local.LocalSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.S3SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.trinofs.TrinoFsSpooledChunkReader;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobSpoolingConfig;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.azure.AzuriteBlobStorage;
import io.starburst.stargate.buffer.data.spooling.gcs.GcsClientConfig;
import io.starburst.stargate.buffer.data.spooling.local.LocalSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.MinioStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3Utils;
import io.starburst.stargate.buffer.data.spooling.trinofs.TrinoFsSpoolingStorage;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.azure.AzuriteHierarchicalNamespaceChecker;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public final class SpoolTestHelper
{
    private SpoolTestHelper() {}

    public static SpoolingStorage createS3SpoolingStorage(MinioStorage minioStorage)
    {
        return createS3SpoolingStorage(minioStorage, "");
    }

    public static SpoolingStorage createS3SpoolingStorage(MinioStorage minioStorage, String path)
    {
        try {
            return new S3SpoolingStorage(
                    new BufferNodeId(0L),
                    new SpoolingDirectoryConfig().setSpoolingDirectory("s3://" + minioStorage.getBucketName() + (path.isEmpty() ? "" : "/" + path)),
                    S3Utils.createS3Client(new S3ClientConfig()
                            .setS3AwsAccessKey(MinioStorage.ACCESS_KEY)
                            .setS3AwsSecretKey(MinioStorage.SECRET_KEY)
                            .setRegion("us-east-1")
                            .setS3Endpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint())),
                    new MergedFileNameGenerator(),
                    new DataServerStats(),
                    S3SpoolingStorage.CompatibilityMode.AWS,
                    new GcsClientConfig());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static SpooledChunkReader createS3SpooledChunkReader(MinioStorage minioStorage, ExecutorService executor)
    {
        return new S3SpooledChunkReader(
                S3Utils.createS3Client(new S3ClientConfig()
                        .setS3AwsAccessKey(MinioStorage.ACCESS_KEY)
                        .setS3AwsSecretKey(MinioStorage.SECRET_KEY)
                        .setRegion("us-east-1")
                        .setS3Endpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint())),
                new DataApiConfig(),
                executor);
    }

    public static SpoolingStorage createAzureBlobSpoolingStorage(BlobServiceAsyncClient client, String containerName)
    {
        return createAzureBlobSpoolingStorage(client, containerName, "");
    }

    public static SpoolingStorage createAzureBlobSpoolingStorage(BlobServiceAsyncClient client, String containerName, String path)
    {
        return new AzureBlobSpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory("abfs://" + containerName + "@test.dfs.core.windows.net" + (path.isEmpty() ? "" : "/" + path)),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                client,
                new AzureBlobSpoolingConfig());
    }

    public static SpooledChunkReader createAzureBlobSpooledChunkReader(BlobServiceAsyncClient client)
    {
        return new AzureBlobSpooledChunkReader(
                new DataApiConfig(),
                client);
    }

    public static SpoolingStorage createLocalSpoolingStorage()
    {
        return new LocalSpoolingStorage(
                new SpoolingDirectoryConfig().setSpoolingDirectory(System.getProperty("java.io.tmpdir") + "/spooling-storage"),
                new MergedFileNameGenerator());
    }

    public static SpooledChunkReader createLocalSpooledChunkReader()
    {
        return new LocalSpooledChunkReader(new DataApiConfig());
    }

    public static SpoolingStorage createTrinoFsS3SpoolingStorage(MinioStorage minioStorage)
    {
        return createTrinoFsS3SpoolingStorage(minioStorage, "");
    }

    public static SpoolingStorage createTrinoFsS3SpoolingStorage(MinioStorage minioStorage, String path)
    {
        return newTrinoFsSpoolingStorage(
                "s3://" + minioStorage.getBucketName() + spoolingDirectorySuffix(path),
                createMinioTrinoFileSystem(minioStorage));
    }

    public static SpooledChunkReader createTrinoFsS3SpooledChunkReader(MinioStorage minioStorage)
    {
        return new TrinoFsSpooledChunkReader(
                new DataApiConfig(),
                createMinioTrinoFileSystem(minioStorage),
                newDirectExecutorService());
    }

    public static SpoolingStorage createTrinoFsAzureBlobSpoolingStorage(AzuriteBlobStorage azurite, String containerName)
    {
        return createTrinoFsAzureBlobSpoolingStorage(azurite, containerName, "");
    }

    public static SpoolingStorage createTrinoFsAzureBlobSpoolingStorage(AzuriteBlobStorage azurite, String containerName, String path)
    {
        return newTrinoFsSpoolingStorage(
                "abfs://" + containerName + "@" + AzuriteBlobStorage.ACCOUNT + ".dfs.core.windows.net" + spoolingDirectorySuffix(path),
                createAzuriteTrinoFileSystem(azurite));
    }

    public static SpooledChunkReader createTrinoFsAzureBlobSpooledChunkReader(AzuriteBlobStorage azurite)
    {
        return new TrinoFsSpooledChunkReader(
                new DataApiConfig(),
                createAzuriteTrinoFileSystem(azurite),
                newDirectExecutorService());
    }

    public static SpoolingStorage createTrinoFsLocalSpoolingStorage(Path rootPath)
    {
        return newTrinoFsSpoolingStorage("local:///", createLocalTrinoFileSystem(rootPath));
    }

    public static SpooledChunkReader createTrinoFsLocalSpooledChunkReader(Path rootPath)
    {
        return new TrinoFsSpooledChunkReader(
                new DataApiConfig(),
                createLocalTrinoFileSystem(rootPath),
                newDirectExecutorService());
    }

    private static String spoolingDirectorySuffix(String path)
    {
        return path.isEmpty() ? "/" : "/" + path + "/";
    }

    private static SpoolingStorage newTrinoFsSpoolingStorage(String spoolingDirectory, TrinoFileSystem fileSystem)
    {
        return new TrinoFsSpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory(spoolingDirectory),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                fileSystem,
                newDirectExecutorService(),
                newDirectExecutorService());
    }

    private static TrinoFileSystem createMinioTrinoFileSystem(MinioStorage minioStorage)
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setEndpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint())
                .setRegion("us-east-1")
                .setPathStyleAccess(true)
                .setAwsAccessKey(MinioStorage.ACCESS_KEY)
                .setAwsSecretKey(MinioStorage.SECRET_KEY);
        return new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats())
                .create(ConnectorIdentity.ofUser("buffer"));
    }

    private static TrinoFileSystem createAzuriteTrinoFileSystem(AzuriteBlobStorage azurite)
    {
        AzureFileSystemConfig config = new AzureFileSystemConfig()
                .setAuthType(AzureFileSystemConfig.AuthType.ACCESS_KEY)
                .setTestingEndpointOverride(azurite.getBlobEndpoint());
        return new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                new AzureAuthAccessKey(AzuriteBlobStorage.ACCOUNT_KEY),
                config,
                Optional.of(new AzuriteHierarchicalNamespaceChecker()))
                .create(ConnectorIdentity.ofUser("buffer"));
    }

    private static TrinoFileSystem createLocalTrinoFileSystem(Path rootPath)
    {
        return new LocalFileSystemFactory(rootPath).create(ConnectorIdentity.ofUser("buffer"));
    }
}
