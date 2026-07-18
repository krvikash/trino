/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // DynamoDB may miss new tables when running in multi threads
final class TestDynamoDbAuthentication
        extends AbstractTestQueryFramework
{
    private static final String AWS_SECRET_KEY = "correctKey";
    private static final String AWS_ACCESS_KEY = "correctKey";

    @TempDir
    private static Path schemaDirectory;

    private TestingDynamoDbServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingDynamoDbServer());

        return DynamoDbQueryRunner.builder()
                .setSchemaDirectory(schemaDirectory)
                .setEndpointUrl(server.getEndpointUrl())
                .setAwsSecretKey(AWS_SECRET_KEY)
                .setAwsAccessKey(AWS_ACCESS_KEY)
                .setTables(ImmutableList.of(NATION))
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .enableWrites()
                .build();
    }

    @Test
    void testQueryWithCorrectCredentials()
    {
        assertQuerySucceeds("SHOW TABLES");
    }

    @Test
    void testQueryWithIncorrectCredentials()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = DynamoDbQueryRunner.builder()
                .setEndpointUrl(server.getEndpointUrl())
                .setTables(ImmutableList.of())
                .setAwsSecretKey("incorrect-key")
                .setAwsAccessKey("incorrect-key")
                .build()) {
            assertThatThrownBy(() -> queryRunner.execute("SHOW TABLES"))
                    .hasMessageContaining("The Access Key ID or security token is invalid");
        }
    }

    @Test
    void testQueryWithIncorrectSystemPropertyCredentials()
    {
        executeExclusively(() -> {
            try (AutoCloseable _ = new TemporalSystemProperty("aws.accessKeyId", "incorrect-key");
                    AutoCloseable _ = new TemporalSystemProperty("aws.secretAccessKey", "incorrect-key");
                    DistributedQueryRunner queryRunner = DynamoDbQueryRunner.builder()
                            .setEndpointUrl(server.getEndpointUrl())
                            .setTables(ImmutableList.of())
                            .addConnectorProperties(ImmutableMap.of("dynamodb.use-default-aws-chain-provider", "true"))
                            .build()) {
                assertThatThrownBy(() -> queryRunner.execute("SHOW TABLES"))
                        .hasMessageContaining("The Access Key ID or security token is invalid");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testQueryWithCorrectSystemPropertyCredentials(@TempDir Path temporaryDirectory)
    {
        executeExclusively(() -> {
            try (AutoCloseable _ = new TemporalSystemProperty("aws.accessKeyId", AWS_ACCESS_KEY);
                    AutoCloseable _ = new TemporalSystemProperty("aws.secretAccessKey", AWS_SECRET_KEY);
                    DistributedQueryRunner queryRunner = DynamoDbQueryRunner.builder()
                            .setSchemaDirectory(temporaryDirectory)
                            .setEndpointUrl(server.getEndpointUrl())
                            .setTables(ImmutableList.of())
                            .addConnectorProperties(ImmutableMap.of("dynamodb.use-default-aws-chain-provider", "true"))
                            .build()) {
                assertThat(queryRunner.execute("SHOW TABLES").getOnlyValue()).isEqualTo("nation");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static class TemporalSystemProperty
            implements AutoCloseable
    {
        private final String key;

        private TemporalSystemProperty(String key, String value)
        {
            this.key = key;
            System.setProperty(key, value);
        }

        @Override
        public void close()
        {
            System.clearProperty(key);
        }
    }
}
