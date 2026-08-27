/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.type.AbstractType;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Disabled("Temporarily disabled until Salesforce CDP account is updated")
final class TestSalesforceCdpConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SalesforceCdpQueryRunner.builder().build();
    }

    @Test
    void testShowCatalogs()
    {
        assertThat((String) computeScalar("SHOW CATALOGS LIKE '%salesforce%'")).isEqualTo("salesforce_cdp");
    }

    @Test
    void testSelectVariousTypes()
    {
        List<AbstractType> tableTypes = List.of(BOOLEAN, VARCHAR, VARCHAR, VARCHAR, DATE, TIMESTAMP_TZ_MILLIS, VARCHAR, VARCHAR, VARCHAR, DOUBLE, DOUBLE, VARCHAR, VARCHAR, VARCHAR);

        MaterializedRow valRow = new MaterializedRow(
                DEFAULT_PRECISION,
                true,
                null,
                "AwsS3_s3_connection",
                "s3://salesforce-test-cdp/types.csv",
                LocalDate.of(2023, 1, 1),
                LocalDateTime.of(2022, 6, 15, 5, 0, 0, 567000000).atZone(ZoneId.of("UTC")),
                "test@test.com",
                null,
                null,
                1.5,
                -6.37,
                "+42 012 345 678",
                "val",
                "http://test.com");

        MaterializedRow nullRow = new MaterializedRow(
                DEFAULT_PRECISION,
                null,
                null,
                "AwsS3_s3_connection",
                "s3://salesforce-test-cdp/types.csv",
                null,
                null,
                "",
                null,
                null,
                2.0,
                null,
                "",
                "",
                "");

        MaterializedRow nullStringRow = new MaterializedRow(
                DEFAULT_PRECISION,
                null,
                null,
                "AwsS3_s3_connection",
                "s3://salesforce-test-cdp/types.csv",
                null,
                null,
                "null",
                null,
                null,
                3.0,
                null,
                "null",
                "null",
                "null");

        MaterializedResult expectedAllRows = resultBuilder(getSession(), tableTypes)
                .rows(List.of(nullRow, nullStringRow, valRow))
                .build();

        MaterializedResult expectedNullsRow = resultBuilder(getSession(), tableTypes)
                .rows(List.of(nullRow, nullStringRow))
                .build();

        MaterializedResult expectedNullRow = resultBuilder(getSession(), tableTypes)
                .rows(List.of(nullRow))
                .build();

        MaterializedResult expectedNullStringRow = resultBuilder(getSession(), tableTypes)
                .rows(List.of(nullStringRow))
                .build();

        MaterializedResult expectedValRow = resultBuilder(getSession(), tableTypes)
                .rows(List.of(valRow))
                .build();

        assertThat(computeActual("SELECT * FROM types")).isEqualTo(expectedAllRows);

        assertThat(computeActual("SELECT * FROM types WHERE boolean = true")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE boolean IS NULL")).isEqualTo(expectedNullsRow);
        assertThat(computeActual("SELECT * FROM types WHERE boolean IS NOT NULL")).isEqualTo(expectedValRow);

        assertThat(computeActual("SELECT * FROM types WHERE email = 'test@test.com'")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE email LIKE '%test%'")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE email IS NULL")).isEmpty();
        assertThat(computeActual("SELECT * FROM types WHERE email IS NOT NULL")).isEqualTo(expectedAllRows);
        assertThat(computeActual("SELECT * FROM types WHERE email = ''")).isEqualTo(expectedNullRow);
        assertThat(computeActual("SELECT * FROM types WHERE email = 'null'")).isEqualTo(expectedNullStringRow);

        assertThat(computeActual("SELECT * FROM types WHERE phone IS NULL")).isEmpty();
        assertThat(computeActual("SELECT * FROM types WHERE phone IS NOT NULL")).isEqualTo(expectedAllRows);
        assertThat(computeActual("SELECT * FROM types WHERE phone = ''")).isEqualTo(expectedNullRow);
        assertThat(computeActual("SELECT * FROM types WHERE phone = 'null'")).isEqualTo(expectedNullStringRow);

        assertThat(computeActual("SELECT * FROM types WHERE url IS NULL")).isEmpty();
        assertThat(computeActual("SELECT * FROM types WHERE url IS NOT NULL")).isEqualTo(expectedAllRows);
        assertThat(computeActual("SELECT * FROM types WHERE url = ''")).isEqualTo(expectedNullRow);
        assertThat(computeActual("SELECT * FROM types WHERE url = 'null'")).isEqualTo(expectedNullStringRow);

        assertThat(computeActual("SELECT * FROM types WHERE text IS NULL")).isEmpty();
        assertThat(computeActual("SELECT * FROM types WHERE text IS NOT NULL")).isEqualTo(expectedAllRows);
        assertThat(computeActual("SELECT * FROM types WHERE text = ''")).isEqualTo(expectedNullRow);
        assertThat(computeActual("SELECT * FROM types WHERE text = 'null'")).isEqualTo(expectedNullStringRow);

        assertThat(computeActual("SELECT * FROM types WHERE internalorganization IS NULL")).isEqualTo(expectedAllRows);
        assertThat(computeActual("SELECT * FROM types WHERE internalorganization IS NOT NULL")).isEmpty();

        assertThat(computeActual("SELECT * FROM types WHERE percent = -6.37")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE percent IS NULL")).isEqualTo(expectedNullsRow);
        assertThat(computeActual("SELECT * FROM types WHERE percent IS NOT NULL")).isEqualTo(expectedValRow);

        assertThat(computeActual("SELECT * FROM types WHERE cast('2023-01-01' as date) = date")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE date IS NULL")).isEqualTo(expectedNullsRow);
        assertThat(computeActual("SELECT * FROM types WHERE date IS NOT NULL")).isEqualTo(expectedValRow);

        assertThat(computeActual("SELECT * FROM types WHERE datetime = CAST('2022-06-15 05:00:00.567 UTC' AS TIMESTAMP WITH TIME ZONE)")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE datetime = CAST('2022-06-14 22:00:00.567 America/Los_Angeles' AS TIMESTAMP WITH TIME ZONE)")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE datetime = TIMESTAMP '2022-06-15 05:00:00.567 UTC'")).isEqualTo(expectedValRow);
        assertThat(computeActual("SELECT * FROM types WHERE datetime IS NULL")).isEqualTo(expectedNullsRow);
        assertThat(computeActual("SELECT * FROM types WHERE datetime IS NOT NULL")).isEqualTo(expectedValRow);

        assertQuery("SELECT count(*) FROM salesforce_cdp.queryservice.orders WHERE orderdate = cast('1996-01-02' as date)", "VALUES 2");
        assertThat(query("SELECT clerk FROM salesforce_cdp.queryservice.orders WHERE orderdate = cast('1996-01-02' as date) and orderkey = 1"))
                .skippingTypesCheck()
                .matches("VALUES ('Clerk#000000951')")
                .isFullyPushedDown();
    }

    @Test
    void testTableDefinitionVariousTypes()
    {
        String tableDefinition =
                """
                VALUES ('cdp_sys_sourceversion' ,'varchar','',' '),
                ('datasource' ,'varchar','',' '),
                ('datasourceobject' ,'varchar','',' '),
                ('internalorganization' ,'varchar','',' '),
                ('boolean' ,'boolean','',' '),
                ('date' ,'date','',' '),
                ('datetime' ,'timestamp(3) with time zone','',' '),
                ('email' ,'varchar','',' '),
                ('kq_number' ,'varchar','',' '),
                ('number' ,'double','',' '),
                ('percent' ,'double','',' '),
                ('phone' ,'varchar','',' '),
                ('text' ,'varchar','',' '),
                ('url' ,'varchar','',' ')""";

        assertQuery("SHOW COLUMNS FROM salesforce_cdp.queryservice.types", tableDefinition);
        assertQuery("DESCRIBE salesforce_cdp.queryservice.types", tableDefinition);
    }

    @Test
    void testCaseSensitivity()
    {
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, DOUBLE)
                .row("A", 1.0)
                .row("B", 2.0)
                .row("a", 3.0)
                .row("", 4.0)
                .build();

        assertThat(query("SELECT teXt, nuMber FROM salesforce_cdp.queryservice.CaseSensitive")).result().matches(expected);

        // without pushdown, i.e. test read data mapping
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE rand() = 42 OR text = 'A'", "VALUES 1.0");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE rand() = 42 OR text = 'a'", "VALUES 3.0");

        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE text = 'B'", "VALUES 2.0");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE text != 'B'", "VALUES (1.0),(3.0),(4.0)");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE text < 'B'", "VALUES (1.0),(4.0)");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE text <= 'B'", "VALUES (1.0),(2.0),(4.0)");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE text > 'B'", "VALUES 3.0");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.CaseSensitive WHERE text >= 'B'", "VALUES (2.0),(3.0)");
    }

    @Test
    void testSelectSpecialCharacters()
    {
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, DOUBLE)
                .row("'", 1.0)
                .row("''", 2.0)
                .row("'''", 3.0)
                .row("\"", 4.0)
                .row("\"\"", 5.0)
                .row("\"\"\"", 6.0)
                .row("`", 7.0)
                .row("``", 8.0)
                .row("```", 9.0)
                .row("?", 10.0)
                .row("/", 11.0)
                .row("//", 12.0)
                .row("///", 13.0)
                .row("\\", 14.0)
                .row("\\\\", 15.0)
                .row("\\\\\\", 16.0)
                .build();

        // Salesforce API column name for t?ext in source csv is returned as t_ext.
        assertThat(query("SELECT t_ext, number FROM salesforce_cdp.queryservice.special_chars")).result().matches(expected);

        assertQuery("SELECT number FROM salesforce_cdp.queryservice.special_chars WHERE t_ext = ''''", "VALUES (1.0)");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.special_chars WHERE t_ext = '\"'", "VALUES (4.0)");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.special_chars WHERE t_ext = '?'", "VALUES (10.0)");
        assertQuery("SELECT number FROM salesforce_cdp.queryservice.special_chars WHERE t_ext = '\\'", "VALUES (14.0)");
    }

    @Test
    @Override // Salesforce adds system columns to each table
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(format(
                        """
                        CREATE TABLE %s.%s.orders (
                           cdp_sys_sourceversion varchar COMMENT ' ',
                           clerk varchar COMMENT ' ',
                           comment varchar COMMENT ' ',
                           custkey double COMMENT ' ',
                           datasource varchar COMMENT ' ',
                           datasourceobject varchar COMMENT ' ',
                           internalorganization varchar COMMENT ' ',
                           kq_orderkey varchar COMMENT ' ',
                           orderdate date COMMENT ' ',
                           orderkey double COMMENT ' ',
                           orderpriority varchar COMMENT ' ',
                           orderstatus varchar COMMENT ' ',
                           shippriority double COMMENT ' ',
                           totalprice double COMMENT ' '
                        )""",
                        catalog,
                        schema));
    }

    @Test
    @Override // Salesforce adds system columns to each table and have only double as format number
    public void testShowColumns()
    {
        assertThat(computeActual("SHOW COLUMNS FROM orders")).isEqualTo(getDescribeOrdersResult());
    }

    @Override
    @Test
    public void testCharTrailingSpace()
    {
        // This test requires table creation but Salesforce Customer Data Platform is read only
    }

    @Override // Salesforce support only double numbers. Changed predicate to reflect that
    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'double' AND table_name = 'nation' and column_name = 'nationkey' LIMIT 1",
                "SELECT 'nation' table_name");
    }

    @Test
    @Override
    public void testShowSchemasFromOther()
    {
        // Salesforce support only one schema
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as Salesforce has an additional columns
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders ORDER BY orderkey LIMIT  5");
    }

    @Test
    @Override // Salesforce validates object name thus adjustment to error type and message
    public void testNoDataSystemTable()
    {
        assertQuerySucceeds("TABLE nation");
        assertThatThrownBy(() -> computeActual("TABLE \"nation$data\""))
                .hasMessageContaining("Invalid object name");
    }

    @Test
    @Override
    public void testExecuteProcedure()
    {
        // Salesforce CDP JDBC driver supports only SELECT statements
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        // Salesforce CDP JDBC driver supports only SELECT statements
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return null;
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("cdp_sys_sourceversion", "varchar", "", " ")
                .row("clerk", "varchar", "", " ")
                .row("comment", "varchar", "", " ")
                .row("custkey", "double", "", " ")
                .row("datasource", "varchar", "", " ")
                .row("datasourceobject", "varchar", "", " ")
                .row("internalorganization", "varchar", "", " ")
                .row("kq_orderkey", "varchar", "", " ")
                .row("orderdate", "date", "", " ")
                .row("orderkey", "double", "", " ")
                .row("orderpriority", "varchar", "", " ")
                .row("orderstatus", "varchar", "", " ")
                .row("shippriority", "double", "", " ")
                .row("totalprice", "double", "", " ")
                .build();
    }

    @Override
    protected @Language("SQL") String getOrdersTableWithColumns()
    {
        return "VALUES " +
                "('orders', 'cdp_sys_sourceversion'), " +
                "('orders', 'datasource'), " +
                "('orders', 'datasourceobject'), " +
                "('orders', 'internalorganization'), " +
                "('orders', 'kq_orderkey'), " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";
    }
}
