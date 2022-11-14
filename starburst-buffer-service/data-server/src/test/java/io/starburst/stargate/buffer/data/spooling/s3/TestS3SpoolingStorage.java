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

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import org.junit.jupiter.api.AfterAll;

import static java.util.UUID.randomUUID;

public class TestS3SpoolingStorage
        extends AbstractTestSpoolingStorage
{
    private MinioStorage minioStorage;

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        this.minioStorage = new MinioStorage("spooling-storage-" + randomUUID());
        minioStorage.start();

        return createSpoolingStorage(ImmutableMap.<String, String>builder()
                .put("discovery-service.uri", "http://dummy") // needed for bootstrap
                .put("spooling.directory", "s3://" + minioStorage.getBucketName())
                .put("spooling.s3.aws-access-key", MinioStorage.ACCESS_KEY)
                .put("spooling.s3.aws-secret-key", MinioStorage.SECRET_KEY)
                .put("spooling.s3.region", "us-east-1")
                .put("spooling.s3.endpoint", "http://" + minioStorage.getMinio().getMinioApiEndpoint())
                .build());
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
