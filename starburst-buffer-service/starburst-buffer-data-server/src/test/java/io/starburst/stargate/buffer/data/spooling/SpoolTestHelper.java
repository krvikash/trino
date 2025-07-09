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
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.azure.AzureBlobSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.local.LocalSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.S3SpooledChunkReader;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobSpoolingConfig;
import io.starburst.stargate.buffer.data.spooling.azure.AzureBlobSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.gcs.GcsClientConfig;
import io.starburst.stargate.buffer.data.spooling.local.LocalSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.MinioStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3ClientConfig;
import io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;

public final class SpoolTestHelper
{
    private SpoolTestHelper() {}

    public static SpoolingStorage createS3SpoolingStorage(MinioStorage minioStorage)
    {
        try {
            return new S3SpoolingStorage(
                    new BufferNodeId(0L),
                    new ChunkManagerConfig().setSpoolingDirectory("s3://" + minioStorage.getBucketName()),
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
        return new AzureBlobSpoolingStorage(
                new BufferNodeId(0L),
                new ChunkManagerConfig().setSpoolingDirectory("abfs://" + containerName + "@test.dfs.core.windows.net"),
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
                new ChunkManagerConfig().setSpoolingDirectory(System.getProperty("java.io.tmpdir") + "/spooling-storage"),
                new MergedFileNameGenerator());
    }

    public static SpooledChunkReader createLocalSpooledChunkReader()
    {
        return new LocalSpooledChunkReader(new DataApiConfig());
    }
}
