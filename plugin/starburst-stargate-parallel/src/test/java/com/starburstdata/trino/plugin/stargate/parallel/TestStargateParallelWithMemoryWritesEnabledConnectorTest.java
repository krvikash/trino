/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.stargate.BaseStargateConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.List;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.parallel.StargateParallelQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestStargateParallelWithMemoryWritesEnabledConnectorTest
        extends BaseStargateConnectorTest
{
    private LocalStackContainer localstack;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        localstack = closeAfterClass(new LocalStackContainer("s3-latest"));
        localstack.start();
        remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithMemory(REQUIRED_TPCH_TABLES, localstack, Optional.empty()));
        return StargateParallelQueryRunner.builder(remoteStarburst, "memory")
                .withEncoding("json") // faster because no decompression
                .enableWrites()
                .build();
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "memory";
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
            case SUPPORTS_UPDATE:
                // not supported in memory connector
                return false;

            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_RENAME_SCHEMA:
                return true;

            case SUPPORTS_DELETE:
                // memory connector does not support deletes
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                // memory connector does not support not-null in create-table
                return true;

            case SUPPORTS_TRUNCATE:
                return false;
            case SUPPORTS_COMMENT_ON_COLUMN:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testSetColumnTypeWithDefaultColumn()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testInsertForDefaultColumn()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        abort("Memory connector does not support truncate");
    }

    @Test
    @Override
    public void testAddColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testAddColumn)
                .hasMessageContaining("This connector does not support adding columns");
        abort("not supported");
    }

    @Test
    @Override
    public void testDropColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("This connector does not support dropping columns");
        abort("not supported");
    }

    @Test
    @Override
    public void testSetColumnType()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testSetColumnType)
                .hasMessageContaining("This connector does not support setting column types");
        abort("not supported");
    }

    @Test
    @Override // override with version from smoke tests to go faster
    public void testInsert()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (42, -38.5)", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override // override with version from smoke tests to go faster
    public void testCreateTableAsSelect()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    // skip larger inputs to go faster
    protected List<Integer> largeInValuesCountData()
    {
        return ImmutableList.of(200);
    }

    @Test
    @Override
    public void verifySupportsDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName(), ".*This connector does not support modifying table rows");
        }
    }

    @Test
    @Override
    public void verifySupportsUpdateDeclaration()
    {
        // TODO: fix/improve me
        assertThatThrownBy(super::verifySupportsUpdateDeclaration)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("This connector does not support modifying table rows");
    }

    @Test
    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        assertThatThrownBy(super::verifySupportsRowLevelDeleteDeclaration)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("This connector does not support modifying table rows");
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        // TODO: fix/improve me
        assertThatThrownBy(super::testNativeQueryCreateStatement)
                .hasMessageContaining("descriptor has no fields");
    }

    @Test
    @Override
    public void testExplainAnalyzePhysicalReadWallTime()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testArithmeticPredicatePushdown()
    {
        // TODO: fix/improve me
        assertThatThrownBy(super::testArithmeticPredicatePushdown)
                .hasMessageContaining("Division by zero");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // TODO: fix/improve me
        assertThatThrownBy(super::testNativeQueryInsertStatementTableExists)
                .hasMessageContaining("mismatched input");
    }

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 a'))", "VALUES 1");
    }

    @Test
    @Override
    public void testDropNotNullConstraint()
    {
        // TODO: fix/improve me
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return ".*NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Test
    @Override // Override because this connector doesn't support creating tables
    public void testExecuteProcedure()
    {
        // TODO (https://github.com/starburstdata/cork/issues/984) Enable this test
    }
}
