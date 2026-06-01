/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.faulttolerant;

import com.google.common.io.Closer;
import com.google.inject.Module;
import com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner;
import com.starburstdata.trino.plugin.snowflake.SnowflakeServer;
import com.starburstdata.trino.plugin.snowflake.TestDatabase;
import io.trino.Session;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.jdbc.BaseJdbcFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;

public abstract class BaseSnowflakeFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    private Closer closer;
    private SqlExecutor snowflakeExecutor;

    public BaseSnowflakeFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected QueryRunner createQueryRunner(List<TpchTable<?>> requiredTpchTables, Map<String, String> configProperties, Map<String, String> coordinatorProperties, Module failureInjectionModule)
            throws Exception
    {
        closer = Closer.create();
        TestDatabase testDB = closer.register(SnowflakeServer.createTestDatabase());
        snowflakeExecutor = sql -> SnowflakeServer.safeExecuteOnDatabase(testDB.getName(), sql);
        return getBuilder()
                .addExtraProperties(configProperties)
                .withConnectorProperties(impersonationDisabled())
                .withDatabase(Optional.of(testDB.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .addCoordinatorProperties(coordinatorProperties)
                .withTpchTables(requiredTpchTables)
                .setAdditionalModule(failureInjectionModule)
                .withExchange("filesystem")
                .build();
    }

    protected SnowflakeQueryRunner.Builder getBuilder()
    {
        return jdbcBuilder();
    }

    @Test
    @Override
    protected void testUpdate()
    {
        // This simple update on JDBC ends up as a very simple, single-fragment, coordinator-only plan,
        // which has no ability to recover from errors. This test simply verifies that's still the case.
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        String testQuery = "UPDATE <table> SET shippriority = 101 WHERE custkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");

        assertThatQuery(testQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .isCoordinatorOnly();
    }

    @Override
    protected void addPrimaryKeyForMergeTarget(Session session, String tableName, String primaryKey)
    {
        String schema = session.getSchema().orElseThrow();
        snowflakeExecutor.execute("ALTER TABLE %s.%s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)".formatted(schema, tableName, tableName, primaryKey));
    }

    @Override
    protected boolean supportsMerge()
    {
        return true;
    }
}
