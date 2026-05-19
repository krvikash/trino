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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDynamoDbConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingDynamoDbServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingDynamoDbServer());
        return DynamoDbQueryRunner.builder(server.getSchemaDirectory())
                .setEndpointUrl(server.getEndpointUrl())
                .setAwsAccessKey("accessKey")
                .setAwsSecretKey("secretKey")
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .enablePredicatePushdown()
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return server::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY -> true;
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_CANCELLATION,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_INSERT,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_MERGE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_NATIVE_QUERY -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "orderkey")
                .row("custkey", "bigint", "", "custkey")
                .row("orderstatus", "varchar(1)", "", "orderstatus")
                .row("totalprice", "double", "", "totalprice")
                .row("orderdate", "date", "", "orderdate")
                .row("orderpriority", "varchar(15)", "", "orderpriority")
                .row("clerk", "varchar(15)", "", "clerk")
                .row("shippriority", "integer", "", "shippriority")
                .row("comment", "varchar(79)", "", "comment")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches(
                        """
                        CREATE TABLE \\w+\\.\\w+\\.orders \\Q(
                           orderkey bigint NOT NULL COMMENT 'orderkey',
                           custkey bigint COMMENT 'custkey',
                           orderstatus varchar(1) COMMENT 'orderstatus',
                           totalprice double COMMENT 'totalprice',
                           orderdate date COMMENT 'orderdate',
                           orderpriority varchar(15) COMMENT 'orderpriority',
                           clerk varchar(15) COMMENT 'clerk',
                           shippriority integer COMMENT 'shippriority',
                           comment varchar(79) COMMENT 'comment'
                        )""");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (ImmutableSet.of("atrailingspace ", " aleadingspace", "a.dot", "a,comma", "a\"quote", "a\\backslash`").contains(columnName)) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.startsWith("decimal") || typeName.equals("char(3)") || typeName.startsWith("time")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "orderkey")
                .row("custkey", "bigint", "", "custkey")
                .row("orderstatus", "varchar(1)", "", "orderstatus")
                .row("totalprice", "double", "", "totalprice")
                .row("orderdate", "date", "", "orderdate")
                .row("orderpriority", "varchar(15)", "", "orderpriority")
                .row("clerk", "varchar(15)", "", "clerk")
                .row("shippriority", "integer", "", "shippriority")
                .row("comment", "varchar(79)", "", "comment")
                .build();

        assertThat(expectedParametrizedVarchar)
                .withFailMessage(format("%s does not match %s", actual, expectedParametrizedVarchar))
                .containsExactlyElementsOf(actual);
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining(
                "SQL compilation error: Non-nullable column 'C_VARCHAR' cannot be added to non-empty table " +
                        "'TEST_ADD_NOTNULL_.*' unless it has a non-null default value\\.");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageMatching("Failed to execute statement: CREATE TABLE amazondynamodb.* \\(x char\\(10\\)\\)");
    }

    @Test
    @Override // Override because this connector doesn't support creating tables
    public void testExecuteProcedure()
    {
        // TODO (https://github.com/starburstdata/cork/issues/984) Enable this test
    }

    @Test
    @Override // Override because this connector doesn't support creating tables
    public void testExecuteProcedureWithNamedArgument()
    {
        // TODO (https://github.com/starburstdata/cork/issues/984) Enable this test
    }
}
