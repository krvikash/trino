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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.starburst.stargate.buffer.data.server.MainModule;
import io.starburst.stargate.buffer.data.spooling.s3.MinioStorage;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class SpoolTestHelper
{
    private SpoolTestHelper() {}

    public static SpoolingStorage createS3SpoolingStorage(MinioStorage minioStorage)
    {
        return createSpoolingStorage(ImmutableMap.<String, String>builder()
                .put("discovery-service.uri", "http://dummy") // needed for bootstrap
                .put("spooling.directory", "s3://" + minioStorage.getBucketName())
                .put("spooling.s3.aws-access-key", MinioStorage.ACCESS_KEY)
                .put("spooling.s3.aws-secret-key", MinioStorage.SECRET_KEY)
                .put("spooling.s3.region", "us-east-1")
                .put("spooling.s3.endpoint", "http://" + minioStorage.getMinio().getMinioApiEndpoint())
                .build());
    }

    public static SpoolingStorage createLocalSpoolingStorage()
    {
        return createSpoolingStorage(ImmutableMap.<String, String>builder()
                .put("discovery-service.uri", "http://dummy") // needed for bootstrap
                .put("spooling.directory", System.getProperty("java.io.tmpdir") + "/spooling-storage")
                .build());
    }

    private static SpoolingStorage createSpoolingStorage(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(new MainModule(0L, false, Ticker.systemTicker()));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(SpoolingStorage.class);
    }
}
