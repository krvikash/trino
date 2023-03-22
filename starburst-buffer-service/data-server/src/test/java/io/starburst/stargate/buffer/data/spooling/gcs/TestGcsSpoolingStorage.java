/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.gcs;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createGcsSpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createGcsSpoolingStorage;

public class TestGcsSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    private FakeGcs fakeGcs;

    @Override
    @BeforeAll
    public void init()
    {
        fakeGcs = new FakeGcs();
        fakeGcs.start();
        super.init();
    }

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        Storage gcsClient = fakeGcs.getGcsClient();
        String bucketName = "spooling-storage-" + UUID.randomUUID();

        gcsClient.create(BucketInfo.of(bucketName));

        return createGcsSpoolingStorage(gcsClient, bucketName);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createGcsSpooledChunkReader(fakeGcs.getGcsClient());
    }

    @Override
    @AfterAll
    public void destroy()
            throws Exception
    {
        super.destroy();
        if (fakeGcs != null) {
            fakeGcs.close();
            fakeGcs = null;
        }
    }
}
