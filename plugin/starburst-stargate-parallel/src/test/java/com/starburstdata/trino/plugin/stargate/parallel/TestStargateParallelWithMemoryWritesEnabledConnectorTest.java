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
import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.stargate.BaseStargateConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static com.starburstdata.trino.plugin.stargate.parallel.StargateParallelQueryRunner.MEMORY_TPCH_SCHEMA;
import static com.starburstdata.trino.plugin.stargate.parallel.StargateParallelQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestStargateParallelWithMemoryWritesEnabledConnectorTest
        extends BaseStargateConnectorTest
{
    private static final String REMOTE_CATALOG_NAME = "memory";
    private static final String CREATE_CATALOG_SQL_TEMPLATE =
            """
            CREATE CATALOG %s USING stargate_parallel
            WITH (
               "connection-url" = '%s',
               "connection-user" = 'p2p'
            )""";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        LocalStackContainer localstack = closeAfterClass(new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.14.0")));
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
        // memory connector does not support deletes or updates
        return switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_SCHEMA -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testCreateDropMultipleCatalogs()
    {
        String firstCatalog = "catalog1_" + randomNameSuffix();
        String secondCatalog = "catalog2_" + randomNameSuffix();
        try {
            @Language("SQL")
            String createFirstCatalogSql = CREATE_CATALOG_SQL_TEMPLATE.formatted(firstCatalog, stargateConnectionUrl(remoteStarburst, REMOTE_CATALOG_NAME));
            assertUpdate(createFirstCatalogSql);
            assertThat(computeScalar("SHOW CREATE CATALOG " + firstCatalog)).isEqualTo(createFirstCatalogSql);
            assertQuerySucceeds("SHOW TABLES FROM %s.%s".formatted(firstCatalog, MEMORY_TPCH_SCHEMA));

            @Language("SQL")
            String createSecondCatalogSql =
                    """
                    CREATE CATALOG %s USING stargate_parallel
                    WITH (
                       "connection-url" = '%s',
                       "connection-user" = 'p2p',
                       "jdbc-types-mapped-to-varchar" = 'ARRAY'
                    )""".formatted(secondCatalog, stargateConnectionUrl(remoteStarburst, REMOTE_CATALOG_NAME));
            assertUpdate(createSecondCatalogSql);
            assertThat(computeScalar("SHOW CREATE CATALOG " + secondCatalog)).isEqualTo(createSecondCatalogSql);
            assertQuerySucceeds("SHOW TABLES FROM %s.%s".formatted(secondCatalog, MEMORY_TPCH_SCHEMA));
        }
        finally {
            assertUpdate("DROP CATALOG IF EXISTS " + firstCatalog);
            assertUpdate("DROP CATALOG IF EXISTS " + secondCatalog);
        }
    }

    @Override
    protected Map<String, String> getBehaviorAlteringCatalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:trino://invalid:8080/hive")
                .buildOrThrow();
    }

    @Override
    protected void assertAlteredCatalogBehavior(String catalogName)
    {
        assertQueryFails("SHOW TABLES FROM %s.%s".formatted(catalogName, MEMORY_TPCH_SCHEMA),
                "Error executing query: java.net.UnknownHostException: invalid.*");
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

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".* Unable to add NOT NULL column '.*' for non-empty table: .*");
    }
}
