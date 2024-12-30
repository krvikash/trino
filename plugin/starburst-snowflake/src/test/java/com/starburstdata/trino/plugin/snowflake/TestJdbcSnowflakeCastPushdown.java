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
import io.trino.testing.QueryRunner;

import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;

public class TestJdbcSnowflakeCastPushdown
        extends BaseSnowflakeCastPushdown
{
    private String testDbName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestDatabase testDb = closeAfterClass(SnowflakeServer.createTestDatabase());
        testDbName = testDb.getName();
        return SnowflakeQueryRunner.jdbcBuilder()
                .withDatabase(Optional.of(testDbName))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(ImmutableMap.of(
                        "jdbc-types-mapped-to-varchar", "c_boolean",
                        "join-pushdown.enabled", "true"))
                .build();
    }

    @Override
    protected String getTestDbName()
    {
        return testDbName;
    }
}
