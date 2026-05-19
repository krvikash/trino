/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestSnowflakeTableStatisticsWithDatabasePrefix
        extends AbstractTestQueryFramework
{
    private TestDatabase testDatabase;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testDatabase = closeAfterClass(SnowflakeServer.createTestDatabase());
        SnowflakeServer.createSchema(testDatabase.getName(), TEST_SCHEMA);
        return createBuilder()
                .withConnectorProperties(impersonationDisabled())
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(ImmutableMap.of(
                        "snowflake.database-prefix-for-schema.enabled", "true"))
                .build();
    }

    protected SnowflakeQueryRunner.Builder<?> createBuilder()
    {
        return jdbcBuilder();
    }

    @Test
    public void testBasic()
    {
        String fullyQualifiedTableName = "\"%s.%s\".test_stats_orders_%s".formatted(testDatabase.getName(), TEST_SCHEMA, randomNameSuffix());
        computeActual(format("CREATE TABLE %s AS SELECT name, nationkey, comment FROM tpch.tiny.nation", fullyQualifiedTableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + fullyQualifiedTableName,
                    """
                    VALUES
                    ('name', NULL, NULL, NULL, NULL, NULL, NULL),
                    ('nationkey', NULL, NULL, NULL, NULL, NULL, NULL),
                    ('comment', NULL, NULL, NULL, NULL, NULL, NULL),
                    (NULL, NULL, NULL, NULL, 25, NULL, NULL)""");
        }
        finally {
            assertUpdate("DROP TABLE " + fullyQualifiedTableName);
        }
    }
}
