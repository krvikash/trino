/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpoolingStorage;

public class TestS3SpoolingStorageWithAncientMinio
        extends AbstractTestS3SpoolingStorage
{
    private static final String ANCIENT_MINIO_IMAGE = "minio/minio:RELEASE.2023-03-20T20-16-18Z";

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    protected String minioImage()
    {
        return ANCIENT_MINIO_IMAGE;
    }

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        // Ancient MinIO does not support aws-chunked content encoding, so it must be disabled
        return createS3SpoolingStorage(minioStorage, false);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createS3SpooledChunkReader(minioStorage, executor);
    }
}
