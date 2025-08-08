/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions;

import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFunctionsPlugin
{
    @Test
    void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                "test",
                        Map.of(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testStorageConfig()
            throws Exception
    {
        Path config = Files.createTempFile("starburst_functions", "json");
        Files.writeString(config, "{\"configurations\":[]}");
        config.toFile().deleteOnExit();

        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                "test",
                        Map.of("io.credentials-file", config.toAbsolutePath().toString()),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testDuplicateLocation()
            throws Exception
    {
        Path config = Files.createTempFile("starburst_functions", "json");
        Files.writeString(config,
                """
                {
                    "configurations": [
                        {
                            "id": "s3",
                            "location": "s3://test-bucket",
                            "configuration": {
                                "s3.region": "us-east-1"
                            }
                        },
                        {
                            "id": "minio",
                            "location": "s3://test-bucket",
                            "configuration": {
                                "s3.region": "us-east-1"
                            }
                        }
                    ]
                }
                """);
        config.toFile().deleteOnExit();

        ConnectorFactory factory = getConnectorFactory();
        assertThatThrownBy(() ->
                factory.create("test",
                                Map.of("io.credentials-file", config.toAbsolutePath().toString()),
                                new TestingConnectorContext())
                        .shutdown())
                .hasStackTraceContaining("Duplicate key \"s3://test-bucket\" found");
    }

    @Test
    void testUnsupportedFileSystem()
            throws Exception
    {
        Path config = Files.createTempFile("starburst_functions", "json");
        Files.writeString(config,
                """
                {
                    "configurations": [
                        {
                            "id": "hdfs",
                            "location": "hdfs://hadoop-master:9000",
                            "configuration": {}
                        }
                    ]
                }
                """);
        config.toFile().deleteOnExit();

        ConnectorFactory factory = getConnectorFactory();
        assertThatThrownBy(() ->
                factory.create("test",
                                Map.of("io.credentials-file", config.toAbsolutePath().toString()),
                                new TestingConnectorContext())
                        .shutdown())
                .hasMessageContaining("Unsupported file system: hdfs://hadoop-master:9000");
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new FunctionsPlugin().getConnectorFactories());
    }
}
