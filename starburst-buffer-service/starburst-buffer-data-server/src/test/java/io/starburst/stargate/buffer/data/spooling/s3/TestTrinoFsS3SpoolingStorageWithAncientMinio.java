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

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsS3SpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsS3SpoolingStorage;

public class TestTrinoFsS3SpoolingStorageWithAncientMinio
        extends AbstractTestS3SpoolingStorage
{
    private static final String ANCIENT_MINIO_IMAGE = "minio/minio:RELEASE.2023-03-20T20-16-18Z";

    @Override
    protected String minioImage()
    {
        return ANCIENT_MINIO_IMAGE;
    }

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        return createTrinoFsS3SpoolingStorage(minioStorage);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createTrinoFsS3SpooledChunkReader(minioStorage);
    }
}
