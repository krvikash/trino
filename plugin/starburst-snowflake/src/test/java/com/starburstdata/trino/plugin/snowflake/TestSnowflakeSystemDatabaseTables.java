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

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSnowflakeSystemDatabaseTables
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        /*
         * Permission setup for SNOWFLAKE_READER role for this test:
         * <p>
         * CREATE ROLE SNOWFLAKE_READER;
         * GRANT ROLE SNOWFLAKE_READER TO USER TEST_USER;
         * GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SNOWFLAKE_READER;
         * GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_ADMIN TO ROLE SNOWFLAKE_READER;
         * GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_VIEWER TO ROLE SNOWFLAKE_READER;
         * GRANT USAGE ON WAREHOUSE TRINO_PLUGINS_TEST_WH TO ROLE SNOWFLAKE_READER;
         * GRANT USAGE ON WAREHOUSE SEP_TEST_WH TO ROLE SNOWFLAKE_READER;
         * GRANT USAGE ON WAREHOUSE TEST_WH TO ROLE SNOWFLAKE_READER;
         */
        return jdbcBuilder()
                .withConnectorProperties(ImmutableMap.of(
                        "snowflake.database-prefix-for-schema.enabled", "true",
                        "snowflake.role", "SNOWFLAKE_READER"))
                .build();
    }

    @Test
    public void testShowTablesInAccountUsage()
    {
        assertThat(computeActual("SHOW TABLES FROM \"snowflake.account_usage\"").getOnlyColumnAsSet())
                .contains("query_history");
    }

    @Test
    public void testShowTablesInTelemetry()
    {
        assertThat(computeActual("SHOW TABLES FROM \"snowflake.telemetry\"").getOnlyColumnAsSet())
                .contains("events", "events_view");
    }

    @Test
    public void testSelectFromAccountUsageView()
    {
        assertThat(query("SELECT * FROM \"snowflake.account_usage\".query_history LIMIT 1"))
                .result()
                .rowCount()
                .isEqualTo(1);
    }

    @Test
    public void testSelectFromTelemetryEventsTable()
    {
        assertThat(query("SELECT * FROM \"snowflake.telemetry\".events LIMIT 0"))
                .result()
                .rowCount()
                .isEqualTo(0);
    }

    @Test
    public void testSelectFromTelemetryEventsView()
    {
        assertThat(query("SELECT * FROM \"snowflake.telemetry\".events_view LIMIT 0"))
                .result()
                .rowCount()
                .isEqualTo(0);
    }
}
