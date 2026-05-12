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

public class TestS3SpoolingStorageWithPath
        extends AbstractTestS3SpoolingStorage
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        return createS3SpoolingStorage(minioStorage, "some/spooling/path");
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createS3SpooledChunkReader(minioStorage, executor);
    }
}
