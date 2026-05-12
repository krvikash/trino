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

import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static java.util.UUID.randomUUID;

public abstract class AbstractTestS3SpoolingStorage
        extends AbstractTestSpoolingStorage
{
    protected MinioStorage minioStorage;

    @Override
    @BeforeAll
    public void init()
    {
        minioStorage = new MinioStorage("spooling-storage-" + randomUUID());
        minioStorage.start();
        super.init();
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
