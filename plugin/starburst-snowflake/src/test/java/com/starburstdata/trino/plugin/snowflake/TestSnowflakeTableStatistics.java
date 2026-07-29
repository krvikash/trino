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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestSnowflakeTableStatistics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestDatabase testDatabase = closeAfterClass(SnowflakeServer.createTestDatabase());
        return parallelBuilder()
                .withConnectorProperties(impersonationDisabled())
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .build();
    }

    @Test
    public void testBasic()
    {
        String tableName = "test_stats_orders_" + randomNameSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT name, nationkey, comment FROM tpch.tiny.nation", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('name', null, null, null, null, null, null)," +
                            "('nationkey', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 25, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
