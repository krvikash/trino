/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.cloud;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemConfig.SignerType;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.security.ConnectorIdentity;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.util.UUID.randomUUID;

public class TestTrinoFsCloudSpoolingStorageOnAwsS3
        extends AbstractTestTrinoFsCloudSpoolingStorage
{
    private S3FileSystemFactory factory;

    @Override
    protected void setupCloudEnvironment()
    {
        String accessKey = requireEnv("AWS_ACCESS_KEY_ID");
        String secretKey = requireEnv("AWS_SECRET_ACCESS_KEY");
        String region = requireEnv("AWS_REGION");
        String bucket = requireEnv("S3_BUCKET");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey(accessKey)
                .setAwsSecretKey(secretKey)
                .setRegion(region)
                .setSignerType(SignerType.AwsS3V4Signer);
        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());
        fileSystem = factory.create(ConnectorIdentity.ofUser("buffer"));
        rootUri = "s3://%s/trino-fs-spool-test-%s/".formatted(bucket, randomUUID());
    }

    @Override
    protected void tearDownCloudEnvironment()
    {
        if (factory != null) {
            try {
                factory.destroy();
            }
            finally {
                factory = null;
            }
        }
    }
}
