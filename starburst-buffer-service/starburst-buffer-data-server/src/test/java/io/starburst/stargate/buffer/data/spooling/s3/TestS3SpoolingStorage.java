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
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import org.junit.jupiter.api.AfterAll;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createS3SpoolingStorage;
import static java.util.UUID.randomUUID;

public class TestS3SpoolingStorage
        extends AbstractTestSpoolingStorage
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private MinioStorage minioStorage;

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        this.minioStorage = new MinioStorage("spooling-storage-" + randomUUID());
        minioStorage.start();

        return createS3SpoolingStorage(minioStorage);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createS3SpooledChunkReader(minioStorage, executor);
    }

    @Override
    @AfterAll
    public void destroy()
            throws Exception
    {
        super.destroy();
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
