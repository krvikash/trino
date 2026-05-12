/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsAzureBlobSpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsAzureBlobSpoolingStorage;

public class TestTrinoFsAzureBlobSpoolingStorageWithPath
        extends AbstractTestAzureBlobSpoolingStorage
{
    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        return createTrinoFsAzureBlobSpoolingStorage(azuriteBlobStorage, containerName, "some/spooling/path");
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createTrinoFsAzureBlobSpooledChunkReader(azuriteBlobStorage);
    }
}
