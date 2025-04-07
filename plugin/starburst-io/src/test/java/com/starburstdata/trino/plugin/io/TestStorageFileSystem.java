/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureFileSystem;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystem;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.filesystem.s3.S3FileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Base64;
import java.util.stream.Stream;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStorageFileSystem
        extends AbstractTestQueryFramework
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String S3_BUCKET = requireEnv("S3_BUCKET");
    private static final String AWS_REGION = requireEnv("AWS_REGION");
    private static final String AWS_ACCESS_KEY_ID = requireEnv("AWS_ACCESS_KEY_ID");
    private static final String AWS_SECRET_ACCESS_KEY = requireEnv("AWS_SECRET_ACCESS_KEY");

    private static final String GCP_STORAGE_BUCKET = requireEnv("GCP_STORAGE_BUCKET");
    private static final String GCP_CREDENTIALS_KEY = new String(Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY")), UTF_8);

    private static final String ABFS_CONTAINER = requireEnv("ABFS_CONTAINER");
    private static final String ABFS_ACCOUNT = requireEnv("ABFS_ACCOUNT");
    private static final String ABFS_ACCESS_KEY = requireEnv("ABFS_ACCESS_KEY");

    private S3FileSystem s3FileSystem;
    private GcsFileSystem gcsFileSystem;
    private AzureFileSystem azureFileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String credentialsKey = BaseEncoding.base64().encode(credentialsKey().getBytes(UTF_8));
        DistributedQueryRunner queryRunner = StorageQueryRunner.builder()
                .addCoordinatorProperty("sql.path", "starburst.io")
                .addConnectorProperty("io.credentials-key", credentialsKey)
                .build();

        ConnectorSession session = queryRunner.getDefaultSession().toConnectorSession();
        s3FileSystem = s3FileSystem(session);
        gcsFileSystem = gcsFileSystem(session);
        azureFileSystem = azureFileSystem(session);

        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("fileSystems")
    void testLoad(TrinoFileSystem fileSystem, Location location)
            throws Exception
    {
        try {
            Location filePath = location.appendPath("region.orc");
            byte[] bytes = Resources.toByteArray(Resources.getResource("tpch_tiny_region.orc"));
            fileSystem.newOutputFile(filePath).createExclusive(bytes);

            assertThat(query("SELECT * FROM TABLE(load('" + location + "/', 'ORC', DESCRIPTOR(regionkey BIGINT, name VARCHAR(25), comment VARCHAR(152))))"))
                    .matches("SELECT * FROM tpch.tiny.region");
            assertThat(query("SELECT * FROM TABLE(load('" + filePath + "', 'ORC', DESCRIPTOR(regionkey BIGINT, name VARCHAR(25), comment VARCHAR(152))))"))
                    .matches("SELECT * FROM tpch.tiny.region");
        }
        finally {
            fileSystem.deleteDirectory(location);
        }
    }

    private Stream<Arguments> fileSystems()
    {
        String suffix = randomNameSuffix();
        return Stream.of(
                Arguments.of(s3FileSystem, Location.of("s3://%s/%s".formatted(S3_BUCKET, suffix))),
                Arguments.of(gcsFileSystem, Location.of("gs://%s/%s".formatted(GCP_STORAGE_BUCKET, suffix))),
                Arguments.of(azureFileSystem, Location.of("abfs://%s@%s.dfs.core.windows.net/%s".formatted(ABFS_CONTAINER, ABFS_ACCOUNT, suffix))));
    }

    private static S3FileSystem s3FileSystem(ConnectorSession session)
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey(AWS_ACCESS_KEY_ID)
                .setAwsSecretKey(AWS_SECRET_ACCESS_KEY)
                .setRegion(AWS_REGION);
        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());
        return (S3FileSystem) fileSystemFactory.create(session);
    }

    private static GcsFileSystem gcsFileSystem(ConnectorSession session)
            throws IOException
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setJsonKey(GCP_CREDENTIALS_KEY);
        GcsFileSystemFactory fileSystemFactory = new GcsFileSystemFactory(config, new GcsStorageFactory(config));
        return (GcsFileSystem) fileSystemFactory.create(session);
    }

    private static AzureFileSystem azureFileSystem(ConnectorSession session)
    {
        AzureFileSystemConfig config = new AzureFileSystemConfig().setAuthType(AzureFileSystemConfig.AuthType.ACCESS_KEY);
        AzureFileSystemFactory fileSystemFactory = new AzureFileSystemFactory(OpenTelemetry.noop(), new AzureAuthAccessKey(ABFS_ACCESS_KEY), config);
        return (AzureFileSystem) fileSystemFactory.create(session);
    }

    private static String credentialsKey()
            throws IOException
    {
        String gcsJsonKey = OBJECT_MAPPER.writeValueAsString(GCP_CREDENTIALS_KEY);
        return """
                {
                    "configurations": [
                        {
                            "id": "s3",
                            "location": "s3://%s",
                            "configuration": {
                                "fs.native-s3.enabled": "true",
                                "s3.aws-access-key": "%s",
                                "s3.aws-secret-key": "%s",
                                "s3.region": "%s"
                            }
                        },
                        {
                            "id": "gcs",
                            "location": "gs://%s",
                            "configuration": {
                                "fs.native-gcs.enabled": "true",
                                "gcs.json-key": %s
                            }
                        },
                        {
                            "id": "azure",
                            "location": "abfs://%s@%s.dfs.core.windows.net",
                            "configuration": {
                                "fs.native-azure.enabled": "true",
                                "azure.auth-type": "ACCESS_KEY",
                                "azure.access-key": "%s"
                            }
                        }
                    ]
                }
                """.formatted(S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, GCP_STORAGE_BUCKET, gcsJsonKey, ABFS_CONTAINER, ABFS_ACCOUNT, ABFS_ACCESS_KEY).stripIndent();
    }
}
