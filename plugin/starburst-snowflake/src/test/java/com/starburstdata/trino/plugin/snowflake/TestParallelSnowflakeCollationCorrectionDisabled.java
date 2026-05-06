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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test for the safety hatch flag for collation fixes.
 * <p>
 * Exercises both code from CollationAwareQueryBuilder (column compared to constant),
 * and code from SnowflakeClient (column compared to column, top n implementation)
 */
public class TestParallelSnowflakeCollationCorrectionDisabled
        extends AbstractTestQueryFramework
{
    private TestDatabase testDatabase;
    private SqlExecutor snowflakeExecutor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testDatabase = closeAfterClass(SnowflakeServer.createTestDatabase());
        snowflakeExecutor = sql -> SnowflakeServer.safeExecuteOnDatabase(testDatabase.getName(), sql);
        return parallelBuilder()
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(Map.of("snowflake.collation-correction.enabled", "false"))
                .build();
    }

    /**
     * @see BaseSnowflakeConnectorTest#testCollatedTopNPushdown()
     */
    @Test
    public void testCollationCorrectionDisabledTopNPushdown()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                schema + ".upper_collated_constant",
                "(a VARCHAR COLLATE 'upper')",
                ImmutableList.of("('a')", "('b')", "('A')", "('B')"))) {
            assertThat(query("SELECT a FROM " + testTable.getName() + " ORDER BY a LIMIT 2"))
                    .skippingTypesCheck()
                    .matches("VALUES ('a'), ('A')");
        }
    }

    /**
     * @see BaseSnowflakeConnectorTest#testCollatedPredicatePushdown()
     */
    @Test
    public void testCollationCorrectionDisabledPredicatePushdown()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                schema + ".upper_collated_variable",
                "(a VARCHAR COLLATE 'upper')",
                ImmutableList.of("('a')"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE a = 'A'"))
                    .result().rowCount().isEqualTo(1);
        }
    }

    /**
     * @see BaseSnowflakeConnectorTest#testCollatedExpression()
     */
    @Test
    public void testCollationCorrectionDisabledEqualsVariable()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                snowflakeExecutor,
                schema + ".upper_collated_variable",
                "(a VARCHAR COLLATE 'upper', b VARCHAR)",
                ImmutableList.of("'a', 'A'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE a = b"))
                    .result().rowCount().isEqualTo(1);
        }
    }

    @Test
    public void testCollationFixDisabledCoalesce()
    {
        try (TestTable table = new TestTable(
                snowflakeExecutor,
                getSession().getSchema().orElseThrow() + ".test_coalesce_collation_collision",
                "(en_col VARCHAR COLLATE 'en', tr_col VARCHAR COLLATE 'tr')",
                List.of("'t', 't'"))) {
            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            assertQueryFails(
                    experimentalPushdownEnabled,
                    "SELECT en_col FROM " + table.getName() + " WHERE COALESCE(en_col, tr_col) = 't'",
                    ".*Incompatible collations.*");
        }
    }

    @Test
    public void testCollationCorrectionDisabledIn()
    {
        try (TestTable table = new TestTable(
                snowflakeExecutor,
                getSession().getSchema().orElseThrow() + ".test_in_collation_collision",
                "(en_col VARCHAR COLLATE 'en', tr_col VARCHAR COLLATE 'tr')",
                List.of("'t', 't'"))) {
            Session experimentalPushdownEnabled = Session.builder(getSession())
                    .setCatalogSessionProperty("snowflake", "experimental_pushdown_enabled", "true")
                    .build();

            assertQueryFails(
                    experimentalPushdownEnabled,
                    "SELECT en_col FROM " + table.getName() + " WHERE 't' IN (en_col, tr_col)",
                    ".*Incompatible collations.*");
        }
    }
}
