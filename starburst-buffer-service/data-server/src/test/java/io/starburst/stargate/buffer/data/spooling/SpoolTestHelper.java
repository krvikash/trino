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

import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.local.LocalSpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.S3SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.s3.SpoolingS3ReaderConfig;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.local.LocalSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.MinioStorage;
import io.starburst.stargate.buffer.data.spooling.s3.S3SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.s3.SpoolingS3Config;

import java.util.concurrent.ExecutorService;

public final class SpoolTestHelper
{
    private SpoolTestHelper() {}

    public static SpoolingStorage createS3SpoolingStorage(MinioStorage minioStorage)
    {
        return new S3SpoolingStorage(
                new BufferNodeId(0L),
                new ChunkManagerConfig().setSpoolingDirectories("s3://" + minioStorage.getBucketName()),
                new SpoolingS3Config()
                        .setS3AwsAccessKey(MinioStorage.ACCESS_KEY)
                        .setS3AwsSecretKey(MinioStorage.SECRET_KEY)
                        .setS3Endpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint()),
                new DataServerStats());
    }

    public static SpooledChunkReader createS3SpooledChunkReader(MinioStorage minioStorage, ExecutorService executor)
    {
        return new S3SpooledChunkReader(
                new SpoolingS3ReaderConfig()
                        .setS3AwsAccessKey(MinioStorage.ACCESS_KEY)
                        .setS3AwsSecretKey(MinioStorage.SECRET_KEY)
                        .setRegion("us-east-1")
                        .setS3Endpoint("http://" + minioStorage.getMinio().getMinioApiEndpoint()),
                new DataApiConfig(),
                executor);
    }

    public static SpoolingStorage createLocalSpoolingStorage()
    {
        return new LocalSpoolingStorage(new ChunkManagerConfig().setSpoolingDirectories(System.getProperty("java.io.tmpdir") + "/spooling-storage"));
    }

    public static SpooledChunkReader createLocalSpooledChunkReader()
    {
        return new LocalSpooledChunkReader(new DataApiConfig());
    }
}
