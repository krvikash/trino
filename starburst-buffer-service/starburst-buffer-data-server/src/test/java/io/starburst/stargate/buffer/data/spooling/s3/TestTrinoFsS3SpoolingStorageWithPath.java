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

public class TestTrinoFsS3SpoolingStorageWithPath
        extends AbstractTestS3SpoolingStorage
{
    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        return createTrinoFsS3SpoolingStorage(minioStorage, "some/spooling/path");
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createTrinoFsS3SpooledChunkReader(minioStorage);
    }
}
