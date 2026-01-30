/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.starburstdata.trino.plugin.functions.FunctionsPlugin;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.starburstdata.trino.plugin.functions.FunctionsMetadata.IO_SCHEMA_NAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

public final class StorageQueryRunner
{
    private StorageQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Plugin plugin = new FunctionsPlugin();
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("starburst")
                    .setSchema(IO_SCHEMA_NAME)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder setPlugin(Plugin plugin)
        {
            this.plugin = plugin;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(plugin);
                queryRunner.createCatalog("starburst", "starburst_functions", connectorProperties);

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static final class StorageDefaultQueryRunnerMain
    {
        static void main()
                throws Exception
        {
            Logging.initialize();

            //noinspection resource
            HiveMinioDataLake container = new Hive3MinioDataLake("test-bucket");
            container.start();

            Path credentialsFile = createCredentialsFile(container.getMinio().getMinioAddress());

            //noinspection resource
            DistributedQueryRunner queryRunner = StorageQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .addCoordinatorProperty("sql.path", "starburst.io")
                    .addConnectorProperty("io.credentials-file", credentialsFile.toAbsolutePath().toString())
                    .build();

            queryRunner.installPlugin(new HivePlugin());
            queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                    .put("hive.metastore.uri", container.getHiveMetastoreEndpoint().toString())
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                    .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", "http://" + container.getMinio().getMinioApiEndpoint())
                    .put("s3.path-style-access", "true")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA hive.tpch WITH (location = 's3://test-bucket/tpch')");

            Logger log = Logger.get(StorageDefaultQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }

        private static Path createCredentialsFile(String minioEndpoint)
                throws IOException
        {
            String json =
                    """
                    {
                        "configurations": [
                            {
                                "id": "minio",
                                "location": "s3://test-bucket",
                                "configuration": {
                                    "fs.native-s3.enabled": "true",
                                    "s3.endpoint": "%s",
                                    "s3.aws-access-key": "accesskey",
                                    "s3.aws-secret-key": "secretkey",
                                    "s3.region": "us-east-1",
                                    "s3.path-style-access": "true"
                                }
                            }
                        ]
                    }
                    """.formatted(minioEndpoint).stripIndent();

            Path config = Files.createTempFile("starburst_functions", "json");
            config.toFile().deleteOnExit();
            Files.writeString(config, json.stripIndent());
            return config;
        }
    }

    public static final class StorageExternalQueryRunnerMain
    {
        // Please set connector properties via VM options. e.g. -Dio.credentials-key=
        static void main()
                throws Exception
        {
            Logging.initialize();

            //noinspection resource
            DistributedQueryRunner queryRunner = StorageQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .addCoordinatorProperty("sql.path", "starburst.io")
                    .build();

            Logger log = Logger.get(StorageExternalQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
