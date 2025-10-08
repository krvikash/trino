/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.clickhouse.BaseClickHouseTypeMapping.JVM_ZONE;
import static io.trino.plugin.clickhouse.BaseClickHouseTypeMapping.KATHMANDU;
import static io.trino.plugin.clickhouse.BaseClickHouseTypeMapping.VILNIUS;
import static io.trino.plugin.clickhouse.ClickHouseSessionProperties.MAP_STRING_AS_VARCHAR;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ORDER_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PARTITION_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.SAMPLE_BY_PROPERTY;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestClickHouseConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_TRUNCATE -> true;
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_MERGE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("clickhouse.map-string-as-varchar", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected Map<String, String> getBehaviorAlteringCatalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-password", "INVALID")
                .buildOrThrow();
    }

    @Override
    protected void assertAlteredCatalogBehavior(String catalogName)
    {
        assertQueryFails(format("SHOW TABLES FROM %s.%s", catalogName, "tiny"),
                "(?s).*Authentication failed: password is incorrect, or there is no user with such name.*");
    }

    @Test
    public void testSampleBySqlInjection()
    {
        String tableName = "sql_injection_" + randomNameSuffix();
        try {
            assertQueryFails("CREATE TABLE " + tableName + " (p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'], sample_by = 'p2; drop table tpch.nation')", "(?s).*Missing columns: 'p2; drop table tpch.nation.*");
            assertUpdate("CREATE TABLE " + tableName + " (p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'], sample_by = 'p2')");
            assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES sample_by = 'p2; drop table tpch.nation'", "(?s).*Missing columns: 'p2; drop table tpch.nation.*");
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES sample_by = 'p2'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        // ClickHouse need resets all data in a column for specified column which to be renamed
        abort("TODO: test not implemented yet");
    }

    @Test
    @Override
    public void testRenameColumnWithComment()
    {
        try (TestTable table = newTrinoTable(
                "test_rename_column_",
                "(id INT NOT NULL, col INT COMMENT 'test column comment') WITH (engine = 'MergeTree', order_by = ARRAY['id'])")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test column comment");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col TO renamed_col");
            assertThat(getColumnComment(table.getName(), "renamed_col")).isEqualTo("test column comment");
        }
    }

    @Override
    public void testAddColumnWithCommentSpecialCharacter(String comment)
    {
        // Override because default storage engine doesn't support renaming columns
        try (TestTable table = newTrinoTable("test_add_column_", "(a_varchar varchar NOT NULL) WITH (engine = 'mergetree', order_by = ARRAY['a_varchar'])")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar COMMENT " + varcharLiteral(comment));
            assertThat(getColumnComment(table.getName(), "b_varchar")).isEqualTo(comment);
        }
    }

    @Test
    @Override
    public void testDropAndAddColumnWithSameName()
    {
        try (TestTable table = newTrinoTable("test_drop_add_column", "(x int NOT NULL, y int, z int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])", ImmutableList.of("1,2,3"))) {
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN y int");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3, NULL)");
        }
    }

    @Override
    protected String createTableSqlForAddingAndDroppingColumn(String tableName, String columnNameInSql)
    {
        return format("CREATE TABLE %s(%s varchar(50), value varchar(50) NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['value'])", tableName, columnNameInSql);
    }

    @Test
    @Disabled
    @Override
    public void testRenameColumnName() {}

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // TODO: Investigate why a\backslash allows creating a table, but it throws an exception when selecting
        if (columnName.equals("a\\backslash`")) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Test
    @Override
    public void testDropColumn()
    {
        String tableName = "test_drop_column_" + randomNameSuffix();

        // only MergeTree engine table can drop column
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['x'], partition_by = ARRAY['a'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);

        // the columns are referenced by order_by/partition_by property can not be dropped
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN x", "(?s).* Missing columns: 'x' while processing: 'x', required columns: 'x'.*");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", "(?s).* Missing columns: 'a' while processing: 'a', required columns: 'a'.*");

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

        assertUpdate("DROP TABLE " + tableName);

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Override
    protected TestTable createTableWithOneIntegerColumn(String namePrefix)
    {
        return newTrinoTable(namePrefix, "(col integer NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['col'])");
    }

    @Override
    protected String tableDefinitionForAddColumn()
    {
        return "(x VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['x'])";
    }

    @Test
    @Override // Overridden because the default storage type doesn't support adding columns
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = newTrinoTable("test_add_notnull_col_to_empty", "(a_varchar varchar NOT NULL)  WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(columnIsNullable(tableName, "b_varchar")).isFalse();
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Test
    @Override // Overridden because (a) the default storage type doesn't support adding columns and (b) ClickHouse has implicit default value for new NON NULL column
    public void testAddNotNullColumn()
    {
        try (TestTable table = newTrinoTable("test_add_notnull_col", "(a_varchar varchar NOT NULL)  WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(columnIsNullable(tableName, "b_varchar")).isFalse();

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);

            // ClickHouse set an empty character as the default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c_varchar varchar NOT NULL");
            assertThat(columnIsNullable(tableName, "c_varchar")).isFalse();
            assertQuery("SELECT c_varchar FROM " + tableName, "VALUES ''");
        }
    }

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        // Override because the default storage type doesn't support adding columns
        try (TestTable table = newTrinoTable("test_add_col_desc_", "(a_varchar varchar NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
            assertThat(getColumnComment(tableName, "b_varchar")).isEqualTo("test new column comment");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN empty_comment varchar COMMENT ''");
            assertThat(getColumnComment(tableName, "empty_comment")).isNull();
        }
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        // TODO: Find the maximum column name length in ClickHouse and enable this test.
        abort("TODO");
    }

    @Test
    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        // TODO: Find the maximum column name length in ClickHouse and enable this test.
        abort("TODO");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE clickhouse.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'LOG'\n" +
                        ")");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.tbl",
                "(col_required Int64," +
                        "col_nullable Nullable(Int64)," +
                        "col_default Nullable(Int64) DEFAULT 43," +
                        "col_nonnull_default Int64 DEFAULT 42," +
                        "col_required2 Int64) ENGINE=Log");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        // TODO run the test with clickhouse.map-string-as-varchar
        abort("");
    }

    @Test
    public void testDifferentEngine()
    {
        String tableName = "test_different_engine_" + randomNameSuffix();
        // MergeTree
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'mergetree', order_by = ARRAY['id'])");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
        // MergeTree without order by
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree')");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);

        // MergeTree with optional
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR, logdate DATE NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['logdate'])");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);

        // Log families
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'log')");
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'tinylog')");
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'stripelog')");
        assertUpdate("DROP TABLE " + tableName);

        // NOT support engine
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'bad_engine')",
                ".* Unable to set catalog 'clickhouse' table property 'engine' to.*");
    }

    @Test
    public void testTableProperty()
    {
        String tableName = "test_table_property_" + randomNameSuffix();
        // no table property, it should create a table with default Log engine table
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR)");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);

        // one required property
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'LOG'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'StripeLog')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'STRIPELOG'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'TinyLog')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'TINYLOG'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Log engine DOES NOT any property
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', order_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', partition_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', sample_by='id')", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");

        // optional properties
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id'],\n" +
                        "   primary_key = ARRAY['id']\n" + // order_by become primary_key automatically in ClickHouse
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // the column refers by order by must be not null
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'])", ".*Sorting key contains nullable columns, but merge tree setting `allow_nullable_key` is disabled.*\\n.*");

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id'],\n" +
                        "   primary_key = ARRAY['id']\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x', 'y'], primary_key = ARRAY['id', 'x'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar NOT NULL,\n" +
                        "   y varchar NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id','x','y'],\n" +
                        "   primary_key = ARRAY['id','x']\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x BOOLEAN NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'], primary_key = ARRAY['id','x'], sample_by = 'x' )");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x boolean NOT NULL,\n" +
                        "   y varchar NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id','x'],\n" +
                        "   primary_key = ARRAY['id','x'],\n" +
                        "   sample_by = 'x'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Partition column
        assertUpdate("CREATE TABLE " + tableName + "(id int NOT NULL, part int NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['part'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   part integer NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id'],\n" +
                        "   partition_by = ARRAY['part'],\n" +
                        "   primary_key = ARRAY['id']\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Primary key must be a prefix of the sorting key,
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x boolean NOT NULL, y boolean NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], sample_by = ARRAY['x', 'y'])",
                ".* Invalid value for catalog 'clickhouse' table property 'sample_by': .*");

        // wrong property type
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = 'id')",
                ".* Invalid value for catalog 'clickhouse' table property 'order_by': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = 'id')",
                ".* Invalid value for catalog 'clickhouse' table property 'primary_key': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'], partition_by = 'id')",
                ".* Invalid value for catalog 'clickhouse' table property 'partition_by': .*");
    }

    @Test
    public void testSetTableProperties()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_alter_table_properties",
                "(p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'])")) {
            assertThat(getTableProperties("tpch", table.getName()))
                    .containsExactlyEntriesOf(ImmutableMap.of(
                            "engine", "MergeTree",
                            "order_by", "p1, p2",
                            "partition_by", "",
                            "primary_key", "p1, p2",
                            "sample_by", ""));

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES sample_by = 'p2'");
            assertThat(getTableProperties("tpch", table.getName()))
                    .containsExactlyEntriesOf(ImmutableMap.of(
                            "engine", "MergeTree",
                            "order_by", "p1, p2",
                            "partition_by", "",
                            "primary_key", "p1, p2",
                            "sample_by", "p2"));
        }
    }

    @Test
    public void testAlterInvalidTableProperties()
    {
        try (TestTable table = newTrinoTable(
                "test_alter_table_properties",
                "(p1 int NOT NULL, p2 int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'])")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES invalid_property = 'p2'",
                    "line 1:66: Catalog 'clickhouse' table property 'invalid_property' does not exist");
        }
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two Map(String, UInt64), three String) ENGINE=Log");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "boolean" -> {
                // ClickHouse does not have built-in support for boolean type and we map boolean to tinyint.
                // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
                yield Optional.empty();
            }
            case "varbinary" -> {
                // here in this test class we map ClickHouse String into varchar, so varbinary ends up a varchar
                yield Optional.empty();
            }
            case "date" -> {
                // The connector supports date type, but these values are unsupported in ClickHouse
                // See BaseClickHouseTypeMapping for additional test coverage
                if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '0001-01-01'") ||
                        dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'") ||
                        dataMappingTestSetup.getHighValueLiteral().equals("DATE '9999-12-31'")) {
                    yield Optional.empty();
                }
                yield Optional.of(dataMappingTestSetup);
            }
            case "time", "time(6)", "timestamp(3) with time zone", "timestamp(6) with time zone" -> Optional.of(dataMappingTestSetup.asUnsupported());
            default -> Optional.of(dataMappingTestSetup);
        };
    }

    // TODO: Remove override once decimal predicate pushdown is implemented (https://github.com/trinodb/trino/issues/7100)
    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = createAggregationTestTable(
                schemaName + ".test_aggregation_pushdown",
                ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(a_bigint), avg(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
        }
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(short_decimal Nullable(Decimal(9, 3)), long_decimal Nullable(Decimal(30, 10)), t_double Nullable(Float64), a_bigint Nullable(Int64)) Engine=Log", rows);
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(t_double Nullable(Float64), u_double Nullable(Float64), v_real Nullable(Float32), w_real Nullable(Float32)) Engine=Log", rows);
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        try (TestTable table = newTrinoTable("test_insert_not_null_", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // ClickHouse inserts default values (e.g. 0 for integer column) even if we don't specify default clause in CREATE TABLE statement
            assertUpdate(format("INSERT INTO %s (nullable_col) VALUES (1)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2), (1, 0)");
        }

        try (TestTable table = newTrinoTable("test_commuted_not_null_table", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }

        try (TestTable table = newTrinoTable("not_null_no_cast", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (TRY(5/0), 4)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col) VALUES (TRY(6/0))", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return "Date must be between 1900-01-01 and 2299-12-31 in ClickHouse: " + date;
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return "Date must be between 1900-01-01 and 2299-12-31 in ClickHouse: " + date;
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                errorMessageForDateYearOfEraPredicate("-1996-09-14"));
    }

    protected String errorMessageForDateYearOfEraPredicate(String date)
    {
        return "Date must be between 1900-01-01 and 2299-12-31 in ClickHouse: " + date;
    }

    @Test
    @Override // ClickHouse char is an alias for String type with no length or padding semantics
    public void testCharTrailingSpace()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(onRemoteDatabase(), schema + ".char_trailing_space", "(x char(10))", List.of("'test'"))) {
            String tableName = table.getName();
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test'", "VALUES 'test'");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test  '", "VALUES 'test'");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test        '", "VALUES 'test'");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char ' test'");
        }
    }

    @Override
    protected TestTable simpleTable()
    {
        // override because Clickhouse requires engine specification
        return new TestTable(onRemoteDatabase(), "tpch.simple_table", "(col BIGINT) Engine=Log", ImmutableList.of("1", "2"));
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*The max length of table name for database tpch is %d, current length is [0-9]+.*\n"
                .formatted(maxTableNameLength().orElseThrow()));
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        // Override because the max length is different from CREATE SCHEMA case
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + sourceTableName);

        String baseSchemaName = "test_rename_target_" + randomNameSuffix();

        // The numeric value depends on file system
        int maxLength = 255 - ".sql".length();

        String validTargetSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + validTargetSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validTargetSchemaName);
        assertUpdate("DROP SCHEMA " + validTargetSchemaName);

        assertUpdate("CREATE SCHEMA " + sourceTableName);
        String invalidTargetSchemaName = validTargetSchemaName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + invalidTargetSchemaName))
                .satisfies(this::verifySchemaNameLengthFailurePermissible);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(invalidTargetSchemaName);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // The numeric value depends on file system
        return OptionalInt.of(255 - ".sql.tmp".length());
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("File name too long");
    }

    @Test
    @Override // Override because the failure message differs
    public void testNativeQueryIncorrectSyntax()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .failure().hasMessage("Query not supported: ResultSetMetaData not available for query: some wrong syntax");
    }

    @Test
    @Override // Override because the failure message differs
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "non_existent_table")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .failure().hasMessage("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
    }

    @Test
    public void testLargeDefaultDomainCompactionThreshold()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String propertyName = catalogName + "." + DOMAIN_COMPACTION_THRESHOLD;
        assertQuery(
                "SHOW SESSION LIKE '" + propertyName + "'",
                "VALUES('" + propertyName + "','1000', '1000', 'integer', 'Maximum ranges to allow in a tuple domain without simplifying it')");
    }

    @Test
    public void testFloatPredicatePushdown()
    {
        try (TestTable table = newTrinoTable(
                "test_float_predicate_pushdown",
                """
                (
                c_real real,
                c_real_neg_infinity real,
                c_real_pos_infinity real,
                c_real_nan real,
                c_double double,
                c_double_neg_infinity double,
                c_double_pos_infinity double,
                c_double_nan double)
                """,
                List.of("3.14, -infinity(), +infinity(), nan(), 3.14, -infinity(), +infinity(), nan()"))) {
            assertThat(query("SELECT c_real FROM %s WHERE c_real = real '3.14'".formatted(table.getName())))
                    // because of https://github.com/trinodb/trino/issues/9998
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT c_real FROM %s WHERE c_real_neg_infinity = -infinity()".formatted(table.getName())))
                    // because of https://github.com/trinodb/trino/issues/9998
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT c_real FROM %s WHERE c_real_pos_infinity = +infinity()".formatted(table.getName())))
                    // because of https://github.com/trinodb/trino/issues/9998
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT c_real FROM %s WHERE c_real_nan = nan()".formatted(table.getName())))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT c_real FROM %s WHERE c_double = double '3.14'".formatted(table.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT c_real FROM %s WHERE c_double_neg_infinity = -infinity()".formatted(table.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT c_real FROM %s WHERE c_double_pos_infinity = +infinity()".formatted(table.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT c_real FROM %s WHERE c_double_nan = nan()".formatted(table.getName())))
                    .isReplacedWithEmptyValues();
        }
    }

    @Test
    public void testOrPredicatePushdown()
    {
        assertThat(query("SELECT * FROM nation WHERE name = 'ALGERIA' OR comment = 'comment'")).isFullyPushedDown();
    }

    @Test
    public void testTextualPredicatePushdown()
    {
        Session smallDomainCompactionThreshold = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", "domain_compaction_threshold", "1")
                .build();

        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA' OR comment = 'P'"))
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name < 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name < 'POLAND' OR comment < 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name <= 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name <= 'POLAND' OR comment <= 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name > 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name > 'POLAND' OR comment > 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name >= 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name >= 'POLAND' OR comment >= 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("clickhouse", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                // Filter node is retained as constraint is pushed into connector is simplified, and
                // The compacted domain is a range predicate which can give wrong results
                // so has to be filtered by trino too to ensure correct predicate.
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania' OR comment = 'P'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        String withConnectorExpression = " OR some_column = 'x'";
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_textual_predicate_pushdown",
                """
                (
                unsupported_1 Point,
                unsupported_2 Point,
                some_column String,
                a_string String,
                a_string_alias Text,
                a_fixed_string FixedString(1),
                a_nullable_string Nullable(String),
                a_nullable_string_alias Nullable(Text),
                a_nullable_fixed_string Nullable(FixedString(1)),
                a_lowcardinality_nullable_string LowCardinality(Nullable(String)),
                a_lowcardinality_nullable_fixed_string LowCardinality(Nullable(FixedString(1))),
                a_enum_1 Enum('hello', 'world', 'a', 'b', 'c', '%', '_'),
                a_enum_2 Enum('hello', 'world', 'a', 'b', 'c', '%', '_'))
                ENGINE=Log
                """,
                List.of(
                        "(10, 10), (10, 10), 'z', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', 'hello', 'world'",
                        "(10, 10), (10, 10), 'z', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_'",
                        "(10, 10), (10, 10), 'z', '%', '%', '%', '%', '%', '%', '%', '%', '%', '%'",
                        "(10, 10), (10, 10), 'z', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a'",
                        "(10, 10), (10, 10), 'z', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b'",
                        "(10, 10), (10, 10), 'z', 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c'"))) {
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string_alias = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string_alias = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_fixed_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_fixed_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string_alias = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string_alias = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_fixed_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_fixed_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_fixed_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_fixed_string = 'b'" + withConnectorExpression)).isFullyPushedDown();

            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_string_alias")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_string_alias" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_enum_1")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_enum_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string = unsupported_1")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string = unsupported_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'hello'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'hello'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'not_a_value'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'not_a_value'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            // pushdown of a condition, both sides of the same native type, which is mapped to varchar,
            // not allowed because some operations (e.g. inequalities) may not be allowed in the native system on an unknown native types
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = a_enum_2")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = a_enum_2" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            // pushdown of a condition, both sides of the same native type, which is mapped to varchar,
            // not allowed because some operations (e.g. inequalities) may not be allowed in the native system on an unknown native types
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 = unsupported_2")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 = unsupported_2" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN (a_string_alias, 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN (a_string_alias, 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            assertLike(true, table, withConnectorExpression, convertToVarchar);
            assertLike(false, table, withConnectorExpression, convertToVarchar);
        }
    }

    private void assertLike(boolean isPositive, TestTable table, String withConnectorExpression, Session convertToVarchar)
    {
        String like = isPositive ? "LIKE" : "NOT LIKE";
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " NULL")).returnsEmptyResult();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_string_alias")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_string_alias" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_enum_1")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_enum_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " unsupported_1")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " unsupported_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        // metacharacters
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '_'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '_'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '__'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '__'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%%'" + withConnectorExpression)).isFullyPushedDown();
        // escape
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\b'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\_'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\_'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\__'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\__'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\' ESCAPE '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\' ESCAPE '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%' ESCAPE '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%' ESCAPE '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%$_%' ESCAPE '$'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%$_%' ESCAPE '$'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    void testDatePredicatePushdown()
    {
        for (ZoneId zoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId.getId()))
                    .build();
            try (TestTable table = new TestTable(
                    onRemoteDatabase(),
                    "tpch.test_date_predicate_pushdown_",
                    "(c_string String, c_date Nullable(Date), c_date32 Nullable(Date32)) Engine = Log",
                    List.of(
                            "'a', '2000-01-01', '2000-01-01'",
                            "'b', '2010-01-01', '2010-01-01'",
                            "'c', '2020-01-01', '2020-01-01'",
                            "'d', null, null"))) {
                // date
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date = DATE '2010-01-01'", "DATE '2010-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date > DATE '2010-01-01'", "DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date < DATE '2010-01-01'", "DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date >= DATE '2010-01-01'", "DATE '2010-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date <= DATE '2010-01-01'", "DATE '2010-01-01', DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date != DATE '2010-01-01'", "DATE '2000-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date IS NULL", "CAST(null AS DATE)");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date IS NOT NULL", "DATE '2000-01-01', DATE '2010-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date IN (DATE '2000-01-01', CAST(null AS DATE))", "DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date BETWEEN DATE '2005-01-01' AND DATE '2015-01-01'", "DATE '2010-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date BETWEEN DATE '2005-01-01' AND DATE '2015-01-01'", "DATE '2010-01-01'");

                // date32
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 = DATE '2010-01-01'", "DATE '2010-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 > DATE '2010-01-01'", "DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 < DATE '2010-01-01'", "DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 >= DATE '2010-01-01'", "DATE '2010-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 <= DATE '2010-01-01'", "DATE '2010-01-01', DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 != DATE '2010-01-01'", "DATE '2000-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 IS NULL", "CAST(null AS DATE)");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 IS NOT NULL", "DATE '2000-01-01', DATE '2010-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 IN (DATE '2000-01-01', CAST(null AS DATE))", "DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date32", "c_date32 BETWEEN DATE '2005-01-01' AND DATE '2015-01-01'", "DATE '2010-01-01'");

                // AND
                assertPredicatePushdown(session, table.getName(), "c_date", "c_string > 'a' AND c_date < DATE '2030-01-01'", "DATE '2010-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_string >= 'a' AND c_date <= DATE '2030-01-01'", "DATE '2000-01-01', DATE '2010-01-01', DATE '2020-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_string = 'a' AND c_date = DATE '2000-01-01'", "DATE '2000-01-01'");
                assertPredicatePushdown(session, table.getName(), "c_date", "c_date >= DATE '2000-01-01' AND c_date32 <= DATE '2010-01-01'", "DATE '2000-01-01', DATE '2010-01-01'");
            }
        }
    }

    @Test
    void testTimestampPredicatePushdown()
    {
        for (ZoneId zoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId.getId()))
                    .build();
            try (TestTable table = new TestTable(
                    onRemoteDatabase(),
                    "tpch.test_datetime_predicate_pushdown_",
                    "(c_string String, c_datetime Nullable(DateTime), c_datetime64 Nullable(DateTime64)) Engine = Log",
                    List.of(
                            "'a', '2000-01-01 11:12:13', '2000-01-01 11:12:13.45'",
                            "'b', '2010-01-01 11:12:13', '2010-01-01 11:12:13.45'",
                            "'c', '2020-01-01 11:12:13', '2020-01-01 11:12:13.45'",
                            "'d', null, null"))) {
                // datetime
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime = TIMESTAMP '2010-01-01 11:12:13'", "TIMESTAMP '2010-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime > TIMESTAMP '2010-01-01 11:12:13'", "TIMESTAMP '2020-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime < TIMESTAMP '2010-01-01 11:12:13'", "TIMESTAMP '2000-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime >= TIMESTAMP '2010-01-01 11:12:13'", "TIMESTAMP '2010-01-01 11:12:13', TIMESTAMP '2020-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime <= TIMESTAMP '2010-01-01 11:12:13'", "TIMESTAMP '2010-01-01 11:12:13', TIMESTAMP '2000-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime != TIMESTAMP '2010-01-01 11:12:13'", "TIMESTAMP '2000-01-01 11:12:13', TIMESTAMP '2020-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime IS NULL", "CAST(null AS TIMESTAMP(0))");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime IS NOT NULL", "TIMESTAMP '2000-01-01 11:12:13', TIMESTAMP '2010-01-01 11:12:13', TIMESTAMP '2020-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime IN (TIMESTAMP '2000-01-01 11:12:13', CAST(null AS TIMESTAMP(0)))", "TIMESTAMP '2000-01-01 11:12:13'");
                assertPredicatePushdown(session, table.getName(), "c_datetime", "c_datetime BETWEEN TIMESTAMP '2005-01-01 11:12:13' AND TIMESTAMP '2015-01-01 11:12:13'", "TIMESTAMP '2010-01-01 11:12:13'");

                // datetime64
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 = TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2010-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 > TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2020-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 < TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2000-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 >= TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2010-01-01 11:12:13.450', TIMESTAMP '2020-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 <= TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2010-01-01 11:12:13.450', TIMESTAMP '2000-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 != TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2000-01-01 11:12:13.450', TIMESTAMP '2020-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 IS NULL", "CAST(null AS TIMESTAMP(3))");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 IS NOT NULL", "TIMESTAMP '2000-01-01 11:12:13.450', TIMESTAMP '2010-01-01 11:12:13.450', TIMESTAMP '2020-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 IN (TIMESTAMP '2000-01-01 11:12:13.450', CAST(null AS TIMESTAMP(3)))", "TIMESTAMP '2000-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 BETWEEN TIMESTAMP '2005-01-01 11:12:13' AND TIMESTAMP '2015-01-01 11:12:13'", "TIMESTAMP '2010-01-01 11:12:13.450'");

                // AND
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_string > 'a' AND c_datetime64 < TIMESTAMP '2030-01-01 11:12:13.450'", "TIMESTAMP '2010-01-01 11:12:13.450', TIMESTAMP '2020-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_string >= 'a' AND c_datetime64 <= TIMESTAMP '2030-01-01 11:12:13.450'", "TIMESTAMP '2000-01-01 11:12:13.450', TIMESTAMP '2010-01-01 11:12:13.450', TIMESTAMP '2020-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_string = 'a' AND c_datetime64 = TIMESTAMP '2000-01-01 11:12:13.450'", "TIMESTAMP '2000-01-01 11:12:13.450'");
                assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 >= TIMESTAMP '2000-01-01 11:12:13.450' AND c_datetime <= TIMESTAMP '2010-01-01 11:12:13.450'", "TIMESTAMP '2000-01-01 11:12:13.450', TIMESTAMP '2010-01-01 11:12:13.450'");
            }

            String digits = "123456789"; // digits to use for building fractional part
            for (int precision = 0; precision < 10; precision++) {
                String fractionPart = precision == 0 ? "" : "." + digits.substring(0, precision);
                try (TestTable table = new TestTable(
                        onRemoteDatabase(),
                        "tpch.test_datetime64_predicate_pushdown_",
                        "(c_datetime64 Nullable(DateTime64(%d))) Engine = Log".formatted(precision),
                        List.of(
                                "'2000-01-01 11:12:13%1$s'".formatted(fractionPart),
                                "'2010-01-01 11:12:13%1$s'".formatted(fractionPart),
                                "'2020-01-01 11:12:13%1$s'".formatted(fractionPart),
                                "null"))) {
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 = TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 > TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart), "TIMESTAMP '2020-01-01 11:12:13%s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 < TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart), "TIMESTAMP '2000-01-01 11:12:13%s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 >= TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%1$s', TIMESTAMP '2020-01-01 11:12:13%1$s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 <= TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%1$s', TIMESTAMP '2000-01-01 11:12:13%1$s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 != TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart), "TIMESTAMP '2000-01-01 11:12:13%1$s', TIMESTAMP '2020-01-01 11:12:13%1$s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 IS NULL", "CAST(null AS TIMESTAMP(%d))".formatted(precision));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 IS NOT NULL", "TIMESTAMP '2000-01-01 11:12:13%1$s', TIMESTAMP '2010-01-01 11:12:13%1$s', TIMESTAMP '2020-01-01 11:12:13%1$s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 IN (TIMESTAMP '2000-01-01 11:12:13%s', CAST(null AS TIMESTAMP(%d)))".formatted(fractionPart, precision), "TIMESTAMP '2000-01-01 11:12:13%s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 BETWEEN TIMESTAMP '2005-01-01 11:12:13%1$s' AND TIMESTAMP '2015-01-01 11:12:13%1$s'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64", "c_datetime64 >= TIMESTAMP '2005-01-01 11:12:13%1$s' AND c_datetime64 <= TIMESTAMP '2015-01-01 11:12:13%1$s'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%s'".formatted(fractionPart));
                }
            }
        }
    }

    @Test
    void testTimestampWithTimeZonePredicatePushdown()
    {
        for (ZoneId zoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId.getId()))
                    .build();
            try (TestTable table = new TestTable(
                    onRemoteDatabase(),
                    "tpch.test_datetime_time_zone_predicate_pushdown_",
                    "(c_datetime_tz Nullable(DateTime('Asia/Kathmandu'))) Engine = Log",
                    List.of(
                            "'2000-01-01 11:12:13'",
                            "'2010-01-01 11:12:13'",
                            "'2020-01-01 11:12:13'",
                            "null"))) {
                // datetime
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz = TIMESTAMP '2010-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2010-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz > TIMESTAMP '2010-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2020-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz < TIMESTAMP '2010-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2000-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz >= TIMESTAMP '2010-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2010-01-01 11:12:13 +05:45', TIMESTAMP '2020-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz <= TIMESTAMP '2010-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2010-01-01 11:12:13 +05:45', TIMESTAMP '2000-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz != TIMESTAMP '2010-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2000-01-01 11:12:13 +05:45', TIMESTAMP '2020-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz IS NULL", "CAST(null AS TIMESTAMP(0) WITH TIME ZONE)");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz IS NOT NULL", "TIMESTAMP '2000-01-01 11:12:13 +05:45', TIMESTAMP '2010-01-01 11:12:13 +05:45', TIMESTAMP '2020-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz IN (TIMESTAMP '2000-01-01 11:12:13 Asia/Kathmandu', CAST(null AS TIMESTAMP(0) WITH TIME ZONE))", "TIMESTAMP '2000-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz BETWEEN TIMESTAMP '2005-01-01 11:12:13 Asia/Kathmandu' AND TIMESTAMP '2015-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2010-01-01 11:12:13 +05:45'");
                assertPredicatePushdown(session, table.getName(), "c_datetime_tz", "c_datetime_tz >= TIMESTAMP '2005-01-01 11:12:13 Asia/Kathmandu' AND c_datetime_tz <= TIMESTAMP '2015-01-01 11:12:13 Asia/Kathmandu'", "TIMESTAMP '2010-01-01 11:12:13 +05:45'");
            }

            String digits = "123456789"; // digits to use for building fractional part
            for (int precision = 0; precision < 10; precision++) {
                String fractionPart = precision == 0 ? "" : "." + digits.substring(0, precision);
                try (TestTable table = new TestTable(
                        onRemoteDatabase(),
                        "tpch.test_datetime64_time_zone_predicate_pushdown_",
                        "(c_datetime64_tz Nullable(DateTime64(%d, 'Asia/Kathmandu'))) Engine = Log".formatted(precision),
                        List.of(
                                "'2000-01-01 11:12:13%1$s'".formatted(fractionPart),
                                "'2010-01-01 11:12:13%1$s'".formatted(fractionPart),
                                "'2020-01-01 11:12:13%1$s'".formatted(fractionPart),
                                "null"))) {
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz = TIMESTAMP '2010-01-01 11:12:13%s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz > TIMESTAMP '2010-01-01 11:12:13%s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2020-01-01 11:12:13%s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz < TIMESTAMP '2010-01-01 11:12:13%s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2000-01-01 11:12:13%s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz >= TIMESTAMP '2010-01-01 11:12:13%s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%1$s +05:45', TIMESTAMP '2020-01-01 11:12:13%1$s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz <= TIMESTAMP '2010-01-01 11:12:13%s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%1$s +05:45', TIMESTAMP '2000-01-01 11:12:13%1$s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz != TIMESTAMP '2010-01-01 11:12:13%s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2000-01-01 11:12:13%1$s +05:45', TIMESTAMP '2020-01-01 11:12:13%1$s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz IS NULL", "CAST(null AS TIMESTAMP(%d) WITH TIME ZONE)".formatted(precision));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz IS NOT NULL", "TIMESTAMP '2000-01-01 11:12:13%1$s +05:45', TIMESTAMP '2010-01-01 11:12:13%1$s +05:45', TIMESTAMP '2020-01-01 11:12:13%1$s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz IN (TIMESTAMP '2000-01-01 11:12:13%s Asia/Kathmandu', CAST(null AS TIMESTAMP(%d) WITH TIME ZONE))".formatted(fractionPart, precision), "TIMESTAMP '2000-01-01 11:12:13%s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz BETWEEN TIMESTAMP '2005-01-01 11:12:13%1$s Asia/Kathmandu' AND TIMESTAMP '2015-01-01 11:12:13%1$s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%s +05:45'".formatted(fractionPart));
                    assertPredicatePushdown(session, table.getName(), "c_datetime64_tz", "c_datetime64_tz >= TIMESTAMP '2005-01-01 11:12:13%1$s Asia/Kathmandu' AND c_datetime64_tz <= TIMESTAMP '2015-01-01 11:12:13%1$s Asia/Kathmandu'".formatted(fractionPart), "TIMESTAMP '2010-01-01 11:12:13%s +05:45'".formatted(fractionPart));
                }
            }
        }
    }

    private void assertPredicatePushdown(Session session, String tableName, String selectColumn, String whereClause, String expectedValue)
    {
        assertThat(query(session, "SELECT %s FROM %s WHERE %s".formatted(selectColumn, tableName, whereClause)))
                .matches("VALUES " + expectedValue)
                .isFullyPushedDown();
    }

    @Test
    public void testIsNull()
    {
        Session mapStringAsVarbinary = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", MAP_STRING_AS_VARCHAR, "false")
                .build();

        NullPushdownDataTypeTest.connectorExpressionOnly()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varchar)")
                .addTestCase("Nullable(real)")
                .addTestCase("Nullable(decimal(3, 1))")
                .addTestCase("Nullable(decimal(30, 5))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_is_null"));

        NullPushdownDataTypeTest.connectorExpressionOnly()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varbinary)")
                .addTestCase("Nullable(char(10))")
                .addTestCase("LowCardinality(Nullable(char(10)))")
                .addTestCase("Nullable(FixedString(10))")
                .addTestCase("LowCardinality(Nullable(FixedString(10)))")
                .addTestCase("Nullable(varchar(30))")
                .addTestCase("LowCardinality(Nullable(varchar(30)))")
                .addTestCase("Nullable(String)")
                .addTestCase("LowCardinality(Nullable(String))")
                .execute(getQueryRunner(), mapStringAsVarbinary, clickhouseCreateAndInsert("tpch.test_is_null"));

        NullPushdownDataTypeTest.create()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varchar)")
                .addTestCase("Nullable(tinyint)")
                .addTestCase("Nullable(smallint)")
                .addTestCase("Nullable(integer)")
                .addTestCase("Nullable(bigint)")
                .addTestCase("Nullable(UInt8)")
                .addTestCase("Nullable(UInt16)")
                .addTestCase("Nullable(UInt32)")
                .addTestCase("Nullable(UInt64)")
                .addTestCase("Nullable(double)")
                .addTestCase("Nullable(char(10))")
                .addTestCase("LowCardinality(Nullable(char(10)))")
                .addTestCase("Nullable(FixedString(10))")
                .addTestCase("LowCardinality(Nullable(FixedString(10)))")
                .addTestCase("Nullable(varchar(30))")
                .addTestCase("LowCardinality(Nullable(varchar(30)))")
                .addTestCase("Nullable(String)")
                .addTestCase("LowCardinality(Nullable(String))")
                .addTestCase("Nullable(date)")
                .addTestCase("Nullable(timestamp)")
                .addTestCase("Nullable(datetime)")
                .addTestCase("Nullable(datetime('UTC'))")
                .addTestCase("Nullable(UUID)")
                .addTestCase("Nullable(IPv4)")
                .addTestCase("Nullable(IPv6)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_is_null"));
    }

    @Test
    void testDateTruncProjectionPushdownWithDateType()
    {
        for (ZoneId zoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId.getId()))
                    .build();
            try (TestTable table = new TestTable(
                    onRemoteDatabase(),
                    "tpch.test_date_trunc_pushdown_with_date_type_",
                    "(c_date Nullable(Date), c_date32 Nullable(Date32)) Engine = Log",
                    List.of(
                            "'2023-01-01', '2023-01-01'",
                            "'2023-06-15', '2023-06-15'",
                            "null, null"))) {
                // date
                assertDateTruncPushdown(session, table.getName(), "c_date", "day", "DATE '2023-01-01', DATE '2023-06-15', null");
                assertDateTruncPushdown(session, table.getName(), "c_date", "week", "DATE '2022-12-26', DATE '2023-06-12', null");
                assertDateTruncPushdown(session, table.getName(), "c_date", "month", "DATE '2023-01-01', DATE '2023-06-01', null");
                assertDateTruncPushdown(session, table.getName(), "c_date", "quarter", "DATE '2023-01-01', DATE '2023-04-01', null");
                assertDateTruncPushdown(session, table.getName(), "c_date", "year", "DATE '2023-01-01', DATE '2023-01-01', null");

                // date32
                assertDateTruncPushdown(session, table.getName(), "c_date32", "day", "DATE '2023-01-01', DATE '2023-06-15', null");
                assertDateTruncPushdown(session, table.getName(), "c_date32", "week", "DATE '2022-12-26', DATE '2023-06-12', null");
                assertDateTruncPushdown(session, table.getName(), "c_date32", "month", "DATE '2023-01-01', DATE '2023-06-01', null");
                assertDateTruncPushdown(session, table.getName(), "c_date32", "quarter", "DATE '2023-01-01', DATE '2023-04-01', null");
                assertDateTruncPushdown(session, table.getName(), "c_date32", "year", "DATE '2023-01-01', DATE '2023-01-01', null");
            }
        }
    }

    @Test
    void testDateTruncProjectionPushdownWithTimestampType()
    {
        for (ZoneId zoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId.getId()))
                    .build();
            try (TestTable table = new TestTable(
                    onRemoteDatabase(),
                    "tpch.test_date_trunc_pushdown_with_timestamp_type_",
                    "(c_datetime Nullable(DateTime), c_datetime64 Nullable(DateTime64), c_datetime64_0 Nullable(DateTime64(0)), c_datetime64_5 Nullable(DateTime64(5)), c_datetime64_9 Nullable(DateTime64(9))) Engine = Log",
                    List.of(
                            "'2023-01-01 12:34:56', '2023-01-01 12:34:56.123', '2023-01-01 12:34:56', '2023-01-01 12:34:56.12345', '2023-01-01 12:34:56.123456789'",
                            "'2023-06-15 23:45:01', '2023-06-15 23:45:01.987', '2023-06-15 23:45:01', '2023-06-15 23:45:01.98765', '2023-06-15 23:45:01.987654321'",
                            "null, null, null, null, null"))) {
                // datetime
                // millisecond unit is not supported with datetime type in ClickHouse
                assertDateTruncNotPushdown(session, table.getName(), "c_datetime", "millisecond", "TIMESTAMP '2023-01-01 12:34:56', TIMESTAMP '2023-06-15 23:45:01', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "second", "TIMESTAMP '2023-01-01 12:34:56', TIMESTAMP '2023-06-15 23:45:01', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "minute", "TIMESTAMP '2023-01-01 12:34:00', TIMESTAMP '2023-06-15 23:45:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "hour", "TIMESTAMP '2023-01-01 12:00:00', TIMESTAMP '2023-06-15 23:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "day", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-06-15 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "week", "TIMESTAMP '2022-12-26 00:00:00', TIMESTAMP '2023-06-12 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "month", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-06-01 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime", "quarter", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-04-01 00:00:00', null");
                assertDateTruncPushdown(table.getName(), "c_datetime", "year", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00', null");

                // datetime64
                // millisecond unit is not pushdown
                assertDateTruncNotPushdown(session, table.getName(), "c_datetime64", "millisecond", "TIMESTAMP '2023-01-01 12:34:56.123', TIMESTAMP '2023-06-15 23:45:01.987', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "second", "TIMESTAMP '2023-01-01 12:34:56.000', TIMESTAMP '2023-06-15 23:45:01.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "minute", "TIMESTAMP '2023-01-01 12:34:00.000', TIMESTAMP '2023-06-15 23:45:00.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "hour", "TIMESTAMP '2023-01-01 12:00:00.000', TIMESTAMP '2023-06-15 23:00:00.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "day", "TIMESTAMP '2023-01-01 00:00:00.000', TIMESTAMP '2023-06-15 00:00:00.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "week", "TIMESTAMP '2022-12-26 00:00:00.000', TIMESTAMP '2023-06-12 00:00:00.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "month", "TIMESTAMP '2023-01-01 00:00:00.000', TIMESTAMP '2023-06-01 00:00:00.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "quarter", "TIMESTAMP '2023-01-01 00:00:00.000', TIMESTAMP '2023-04-01 00:00:00.000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64", "year", "TIMESTAMP '2023-01-01 00:00:00.000', TIMESTAMP '2023-01-01 00:00:00.000', null");

                // datetime64(0)
                // millisecond unit is not pushdown
                assertDateTruncNotPushdown(session, table.getName(), "c_datetime64_0", "millisecond", "TIMESTAMP '2023-01-01 12:34:56', TIMESTAMP '2023-06-15 23:45:01', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "second", "TIMESTAMP '2023-01-01 12:34:56', TIMESTAMP '2023-06-15 23:45:01', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "minute", "TIMESTAMP '2023-01-01 12:34:00', TIMESTAMP '2023-06-15 23:45:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "hour", "TIMESTAMP '2023-01-01 12:00:00', TIMESTAMP '2023-06-15 23:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "day", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-06-15 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "week", "TIMESTAMP '2022-12-26 00:00:00', TIMESTAMP '2023-06-12 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "month", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-06-01 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "quarter", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-04-01 00:00:00', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_0", "year", "TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00', null");

                // datetime64(5)
                // millisecond unit is not pushdown
                assertDateTruncNotPushdown(session, table.getName(), "c_datetime64_5", "millisecond", "TIMESTAMP '2023-01-01 12:34:56.12300', TIMESTAMP '2023-06-15 23:45:01.98700', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "second", "TIMESTAMP '2023-01-01 12:34:56.00000', TIMESTAMP '2023-06-15 23:45:01.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "minute", "TIMESTAMP '2023-01-01 12:34:00.00000', TIMESTAMP '2023-06-15 23:45:00.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "hour", "TIMESTAMP '2023-01-01 12:00:00.00000', TIMESTAMP '2023-06-15 23:00:00.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "day", "TIMESTAMP '2023-01-01 00:00:00.00000', TIMESTAMP '2023-06-15 00:00:00.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "week", "TIMESTAMP '2022-12-26 00:00:00.00000', TIMESTAMP '2023-06-12 00:00:00.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "month", "TIMESTAMP '2023-01-01 00:00:00.00000', TIMESTAMP '2023-06-01 00:00:00.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "quarter", "TIMESTAMP '2023-01-01 00:00:00.00000', TIMESTAMP '2023-04-01 00:00:00.00000', null");
                assertDateTruncPushdown(session, table.getName(), "c_datetime64_5", "year", "TIMESTAMP '2023-01-01 00:00:00.00000', TIMESTAMP '2023-01-01 00:00:00.00000', null");
            }
        }
    }

    private void assertDateTruncPushdown(String tableName, String column, String unit, String expectedValue)
    {
        assertDateTruncPushdown(getSession(), tableName, column, unit, expectedValue);
    }

    private void assertDateTruncPushdown(Session session, String tableName, String column, String unit, String expectedValue)
    {
        assertThat(query(session, "SELECT date_trunc('%s', %s) FROM %s".formatted(unit, column, tableName)))
                .matches("VALUES %s".formatted(expectedValue))
                .isFullyPushedDown();
    }

    private List<ZoneId> timezones()
    {
        return ImmutableList.of(
                ZoneId.of("UTC"),
                JVM_ZONE,
                // using two non-JVM zones so that we don't need to worry what ClickHouse system zone is
                VILNIUS,
                KATHMANDU,
                TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    @Test
    void testDateTruncProjectionPushdownWithTimestampTypeWithTz()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_timestamp_with_tz_type_",
                "(c_datetime_tz Nullable(DateTime('Asia/Kathmandu')), c_datetime64_0_tz Nullable(DateTime64(0, 'Asia/Kathmandu')), c_datetime64_5_tz Nullable(DateTime64(5, 'Asia/Kathmandu')), c_datetime64_9 Nullable(DateTime64(9, 'Asia/Kathmandu'))) Engine = Log",
                List.of(
                        "'2023-01-01 12:34:56', '2023-01-01 12:34:56', '2023-01-01 12:34:56.12345', '2023-01-01 12:34:56.123456789'",
                        "'2023-06-15 23:45:01', '2023-06-15 23:45:01', '2023-06-15 23:45:01.98765', '2023-06-15 23:45:01.987654321'",
                        "null, null, null, null"))) {
            // datetime('<TZ>')
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "millisecond", "TIMESTAMP '2023-01-01 12:34:56 +05:45', TIMESTAMP '2023-06-15 23:45:01 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "second", "TIMESTAMP '2023-01-01 12:34:56 +05:45', TIMESTAMP '2023-06-15 23:45:01 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "minute", "TIMESTAMP '2023-01-01 12:34:00 +05:45', TIMESTAMP '2023-06-15 23:45:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "hour", "TIMESTAMP '2023-01-01 12:00:00 +05:45', TIMESTAMP '2023-06-15 23:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "day", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-06-15 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "week", "TIMESTAMP '2022-12-26 00:00:00 +05:45', TIMESTAMP '2023-06-12 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "month", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-06-01 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "quarter", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-04-01 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime_tz", "year", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-01-01 00:00:00 +05:45', null");

            // datetime64(0, '<TZ>')
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "millisecond", "TIMESTAMP '2023-01-01 12:34:56 +05:45', TIMESTAMP '2023-06-15 23:45:01 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "second", "TIMESTAMP '2023-01-01 12:34:56 +05:45', TIMESTAMP '2023-06-15 23:45:01 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "minute", "TIMESTAMP '2023-01-01 12:34:00 +05:45', TIMESTAMP '2023-06-15 23:45:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "hour", "TIMESTAMP '2023-01-01 12:00:00 +05:45', TIMESTAMP '2023-06-15 23:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "day", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-06-15 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "week", "TIMESTAMP '2022-12-26 00:00:00 +05:45', TIMESTAMP '2023-06-12 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "month", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-06-01 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "quarter", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-04-01 00:00:00 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_0_tz", "year", "TIMESTAMP '2023-01-01 00:00:00 +05:45', TIMESTAMP '2023-01-01 00:00:00 +05:45', null");

            // datetime64(5, '<TZ>')
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "millisecond", "TIMESTAMP '2023-01-01 12:34:56.12300 +05:45', TIMESTAMP '2023-06-15 23:45:01.98700 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "second", "TIMESTAMP '2023-01-01 12:34:56.00000 +05:45', TIMESTAMP '2023-06-15 23:45:01.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "minute", "TIMESTAMP '2023-01-01 12:34:00.00000 +05:45', TIMESTAMP '2023-06-15 23:45:00.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "hour", "TIMESTAMP '2023-01-01 12:00:00.00000 +05:45', TIMESTAMP '2023-06-15 23:00:00.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "day", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45', TIMESTAMP '2023-06-15 00:00:00.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "week", "TIMESTAMP '2022-12-26 00:00:00.00000 +05:45', TIMESTAMP '2023-06-12 00:00:00.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "month", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45', TIMESTAMP '2023-06-01 00:00:00.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "quarter", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45', TIMESTAMP '2023-04-01 00:00:00.00000 +05:45', null");
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_5_tz", "year", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45', TIMESTAMP '2023-01-01 00:00:00.00000 +05:45', null");
        }
    }

    private void assertDateTruncNotPushdown(String tableName, String column, String unit, String expectedValue)
    {
        assertDateTruncNotPushdown(getSession(), tableName, column, unit, expectedValue);
    }

    private void assertDateTruncNotPushdown(Session session, String tableName, String column, String unit, String expectedValue)
    {
        assertThat(query(session, "SELECT date_trunc('%s', %s) FROM %s".formatted(unit, column, tableName)))
                .matches("VALUES %s".formatted(expectedValue))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testDateTruncPredicateNotPushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_date_type_",
                "(c_date Nullable(Date), c_date32 Nullable(Date32), c_datetime64_5 Nullable(DateTime64(5)), c_datetime64_5_tz Nullable(DateTime64(5, 'Asia/Kathmandu'))) Engine = Log",
                List.of(
                        "'2023-01-01', '2023-01-01', '2023-01-01 12:34:56.12345', '2023-01-01 12:34:56.12345'",
                        "'2024-06-15', '2024-06-15', '2024-06-15 23:45:01.98765', '2024-06-15 23:45:01.98765'",
                        "null, null, null, null"))) {
            // date
            assertDateTruncPredicateNotPushdown(table.getName(), "c_date", "day", "DATE '2023-01-01'", ProjectNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_date", "week", "DATE '2022-12-26'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_date", "month", "DATE '2023-01-01'", ProjectNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_date", "quarter", "DATE '2023-01-01'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_date", "year", "DATE '2023-01-01'", ProjectNode.class);

            // datetime64
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "millisecond", "TIMESTAMP '2023-01-01 12:34:56.12300'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "second", "TIMESTAMP '2023-01-01 12:34:56.00000'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "minute", "TIMESTAMP '2023-01-01 12:34:00.00000'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "hour", "TIMESTAMP '2023-01-01 12:00:00.00000'", ProjectNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "day", "TIMESTAMP '2023-01-01 00:00:00.00000'", ProjectNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "week", "TIMESTAMP '2022-12-26 00:00:00.00000'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "month", "TIMESTAMP '2023-01-01 00:00:00.00000'", ProjectNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "quarter", "TIMESTAMP '2023-01-01 00:00:00.00000'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5", "year", "TIMESTAMP '2023-01-01 00:00:00.00000'", ProjectNode.class);

            // datetime64(5, '<TZ>')
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "millisecond", "TIMESTAMP '2023-01-01 12:34:56.12300 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "second", "TIMESTAMP '2023-01-01 12:34:56.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "minute", "TIMESTAMP '2023-01-01 12:34:00.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "hour", "TIMESTAMP '2023-01-01 12:00:00.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "day", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "week", "TIMESTAMP '2022-12-26 00:00:00.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "month", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "quarter", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45'", FilterNode.class);
            assertDateTruncPredicateNotPushdown(table.getName(), "c_datetime64_5_tz", "year", "TIMESTAMP '2023-01-01 00:00:00.00000 +05:45'", FilterNode.class);
        }
    }

    private void assertDateTruncPredicateNotPushdown(String tableName, String column, String unit, String predicateValue, Class<? extends PlanNode> nodeNotPushedDown)
    {
        assertThat(query("SELECT true FROM %s WHERE date_trunc('%s', %s) = %s".formatted(tableName, unit, column, predicateValue)))
                .matches("VALUES true")
                .isNotFullyPushedDown(nodeNotPushedDown);
    }

    @Test
    void testDateTruncProjectionPushdownWithMinAndMaxDate()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_min_max_date_",
                "(c_date Nullable(Date)) Engine = Log",
                List.of(
                        "'1970-01-01'",
                        "'2106-02-07'",
                        "null"))) {
            assertDateTruncPushdown(table.getName(), "c_date", "day", "DATE '1970-01-01', DATE '2106-02-07', null");
            assertDateTruncPushdown(table.getName(), "c_date", "week", "DATE '1969-12-29', DATE '2106-02-01', null");
            assertDateTruncPushdown(table.getName(), "c_date", "month", "DATE '1970-01-01', DATE '2106-02-01', null");
            assertDateTruncPushdown(table.getName(), "c_date", "quarter", "DATE '1970-01-01', DATE '2106-01-01', null");
            assertDateTruncPushdown(table.getName(), "c_date", "year", "DATE '1970-01-01', DATE '2106-01-01', null");
        }
    }

    @Test
    void testDateTruncProjectionPushdownWithMinAndMaxDate32()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_min_max_date32_",
                "(c_date32 Nullable(Date32)) Engine = Log",
                List.of(
                        "'1900-01-01'",
                        "'2299-12-31'",
                        "null"))) {
            assertDateTruncPushdown(table.getName(), "c_date32", "day", "DATE '1900-01-01', DATE '2299-12-31', null");
            assertDateTruncPushdown(table.getName(), "c_date32", "week", "DATE '1900-01-01', DATE '2299-12-25', null");
            assertDateTruncPushdown(table.getName(), "c_date32", "month", "DATE '1900-01-01', DATE '2299-12-01', null");
            assertDateTruncPushdown(table.getName(), "c_date32", "quarter", "DATE '1900-01-01', DATE '2299-10-01', null");
            assertDateTruncPushdown(table.getName(), "c_date32", "year", "DATE '1900-01-01', DATE '2299-01-01', null");
        }
    }

    @Test
    void testDateTruncProjectionPushdownWithMinAndMaxDateTime()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_min_max_datetime_",
                "(c_datetime Nullable(DateTime)) Engine = Log",
                List.of(
                        "'1970-01-01 00:00:00'",
                        "'2106-02-07 06:28:15'",
                        "null"))) {
            assertDateTruncNotPushdown(table.getName(), "c_datetime", "millisecond", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-02-07 06:28:15', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "second", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-02-07 06:28:15', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "minute", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-02-07 06:28:00', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "hour", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-02-07 06:00:00', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "day", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-02-07 00:00:00', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "week", "TIMESTAMP '1969-12-29 00:00:00', TIMESTAMP '2106-02-01 00:00:00', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "month", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-02-01 00:00:00', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "quarter", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-01-01 00:00:00', null");
            assertDateTruncPushdown(table.getName(), "c_datetime", "year", "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2106-01-01 00:00:00', null");
        }
    }

    @Test
    void testDateTruncProjectionPushdownWithMinAndMaxDateTime64()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_min_max_datetime64_8_",
                "(c_datetime64_8 Nullable(DateTime64(8))) Engine = Log",
                List.of(
                        "'1900-01-01 00:00:00.00000000'",
                        "'2299-12-31 23:59:59.99999999'",
                        "null"))) {
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_8", "millisecond", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-31 23:59:59.99900000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "second", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-31 23:59:59.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "minute", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-31 23:59:00.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "hour", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-31 23:00:00.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "day", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-31 00:00:00.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "week", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-25 00:00:00.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "month", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-12-01 00:00:00.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "quarter", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-10-01 00:00:00.00000000', null");
            assertDateTruncPushdown(table.getName(), "c_datetime64_8", "year", "TIMESTAMP '1900-01-01 00:00:00.00000000', TIMESTAMP '2299-01-01 00:00:00.00000000', null");
        }

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_date_trunc_pushdown_with_min_max_datetime64_9_",
                "(c_datetime64_9 DateTime64(9)) Engine = Log",
                List.of("'2262-04-11 23:47:16.854775807'"))) {
            assertDateTruncNotPushdown(table.getName(), "c_datetime64_9", "millisecond", "TIMESTAMP '2262-04-11 23:47:16.854000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "second", "TIMESTAMP '2262-04-11 23:47:16.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "minute", "TIMESTAMP '2262-04-11 23:47:00.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "hour", "TIMESTAMP '2262-04-11 23:00:00.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "day", "TIMESTAMP '2262-04-11 00:00:00.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "week", "TIMESTAMP '2262-04-07 00:00:00.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "month", "TIMESTAMP '2262-04-01 00:00:00.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "quarter", "TIMESTAMP '2262-04-01 00:00:00.000000000'");
            assertDateTruncPushdown(table.getName(), "c_datetime64_9", "year", "TIMESTAMP '2262-01-01 00:00:00.000000000'");
        }
    }

    @Test
    @Override // Override because ClickHouse doesn't follow SQL standard syntax
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CREATE TABLE " + schemaTableName + "(id int NOT NULL, data int) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        try {
            assertUpdate("CALL system.execute('INSERT INTO " + schemaTableName + " VALUES (1, 10)')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 10)");

            assertUpdate("CALL system.execute('ALTER TABLE " + schemaTableName + " UPDATE data = 100 WHERE true')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 100)");

            assertUpdate("CALL system.execute('ALTER TABLE " + schemaTableName + " DELETE WHERE true')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    public void testTimestampExtractionFunctionsPushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_timestamp_extraction_pushdown_",
                """
                (c_date Nullable(Date),
                 c_date32 Nullable(Date32),
                 c_datetime Nullable(DateTime),
                 c_datetime_tz Nullable(DateTime('Europe/Vilnius')),
                 c_datetime64 Nullable(DateTime64),
                 c_datetime64_0 Nullable(DateTime64(0)),
                 c_datetime64_5 Nullable(DateTime64(5)),
                 c_datetime64_3_tz Nullable(DateTime64(3, 'Pacific/Apia')),
                 c_datetime64_9_tz Nullable(DateTime64(9, 'Asia/Kathmandu'))) ENGINE = Log""",
                ImmutableList.of(
                        "'2015-01-01', '2015-01-01', '2015-01-01 12:34:56', '2015-01-01 12:34:56', '2015-01-01 12:34:56.123', '2015-01-01 12:34:56', '2015-01-01 12:34:56.12345', '2015-01-01 12:34:56.123', '2015-01-01 12:34:56.123456789'",
                        "'2016-01-01', '2016-01-01', '2016-01-01 12:34:56', '2016-01-01 12:34:56', '2016-01-01 12:34:56.987', '2016-01-01 12:34:56', '2016-01-01 12:34:56.98765', '2016-01-01 12:34:56.987', '2016-01-01 12:34:56.987654321'",
                        "'2020-12-31', '2020-12-31', '2020-12-31 12:34:56', '2020-12-31 12:34:56', '2020-12-31 12:34:56.123', '2020-12-31 12:34:56', '2020-12-31 12:34:56.12345', '2020-12-31 12:34:56.123', '2020-12-31 12:34:56.123456789'",
                        "'2025-12-31', '2025-12-31', '2025-12-31 12:34:56', '2025-12-31 12:34:56', '2025-12-31 12:34:56.987', '2025-12-31 12:34:56', '2025-12-31 12:34:56.98765', '2025-12-31 12:34:56.987', '2025-12-31 12:34:56.987654321'",
                        "null, null, null, null, null, null, null, null, null"))) {
            String tableName = table.getName();

            assertTimestampExtractionFunctionIsPushdown("year", "c_date", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_date32", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime_tz", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime64", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime64_0", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime64_5", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime64_3_tz", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");
            assertTimestampExtractionFunctionIsPushdown("year", "c_datetime64_9_tz", tableName, "BIGINT '2015', BIGINT '2016', BIGINT '2020', BIGINT '2025', null");

            assertTimestampExtractionFunctionIsPushdown("quarter", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");
            assertTimestampExtractionFunctionIsPushdown("quarter", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '4', BIGINT '4', null");

            assertTimestampExtractionFunctionIsPushdown("month", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("month", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '12', BIGINT '12', null");

            assertTimestampExtractionFunctionIsPushdown("week", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertTimestampExtractionFunctionIsPushdown("week", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");

            assertTimestampExtractionFunctionIsPushdown("day", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");

            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_month", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '31', BIGINT '31', null");

            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_date", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_date32", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime_tz", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime64", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime64_0", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime64_5", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime64_3_tz", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_week", "c_datetime64_9_tz", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");

            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("day_of_year", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");

            assertTimestampExtractionFunctionIsPushdown("dow", "c_date", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_date32", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime_tz", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime64", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime64_0", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime64_5", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime64_3_tz", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");
            assertTimestampExtractionFunctionIsPushdown("dow", "c_datetime64_9_tz", tableName, "BIGINT '4', BIGINT '5', BIGINT '4', BIGINT '3', null");

            assertTimestampExtractionFunctionIsPushdown("doy", "c_date", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_date32", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime64", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime64_0", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime64_5", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime64_3_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");
            assertTimestampExtractionFunctionIsPushdown("doy", "c_datetime64_9_tz", tableName, "BIGINT '1', BIGINT '1', BIGINT '366', BIGINT '365', null");

            // only datetime types are supported for EXTRACT function with field hour
            assertExtractFunctionIsPushdown("hour(c_date)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("hour(c_date32)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime_tz", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime64", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime64_0", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime64_5", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime64_3_tz", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");
            assertTimestampExtractionFunctionIsPushdown("hour", "c_datetime64_9_tz", tableName, "BIGINT '12', BIGINT '12', BIGINT '12', BIGINT '12', null");

            // only datetime types are supported for EXTRACT function with field minute
            assertExtractFunctionIsPushdown("minute(c_date)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("minute(c_date32)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime_tz", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime64", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime64_0", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime64_5", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime64_3_tz", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");
            assertTimestampExtractionFunctionIsPushdown("minute", "c_datetime64_9_tz", tableName, "BIGINT '34', BIGINT '34', BIGINT '34', BIGINT '34', null");

            // only datetime types are supported for EXTRACT function with field second
            assertExtractFunctionIsPushdown("second(c_date)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("second(c_date32)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime_tz", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime64", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime64_0", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime64_5", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime64_3_tz", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");
            assertTimestampExtractionFunctionIsPushdown("second", "c_datetime64_9_tz", tableName, "BIGINT '56', BIGINT '56', BIGINT '56', BIGINT '56', null");

            // millisecond is not supported fields in EXTRACT function
            assertExtractFunctionIsPushdown("millisecond(c_date)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("millisecond(c_date32)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime_tz)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime64)", tableName, "BIGINT '123', BIGINT '987', BIGINT '123', BIGINT '987', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime64_0)", tableName, "BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime64_5)", tableName, "BIGINT '123', BIGINT '987', BIGINT '123', BIGINT '987', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime64_3_tz)", tableName, "BIGINT '123', BIGINT '987', BIGINT '123', BIGINT '987', null");
            assertExtractFunctionIsPushdown("millisecond(c_datetime64_9_tz)", tableName, "BIGINT '123', BIGINT '987', BIGINT '123', BIGINT '987', null");

            // week_of_year is not supported parameter in EXTRACT function
            assertExtractFunctionIsPushdown("week_of_year(c_date)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_date32)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime_tz)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime64)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime64_0)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime64_5)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime64_3_tz)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");
            assertExtractFunctionIsPushdown("week_of_year(c_datetime64_9_tz)", tableName, "BIGINT '1', BIGINT '1', BIGINT '53', BIGINT '53', null");

            // not supported functions: year_of_week, yow
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_date", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_date32", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime_tz", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime64", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime64_0", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime64_5", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime64_3_tz", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("year_of_week", "c_datetime64_9_tz", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");

            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_date", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_date32", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime_tz", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime64", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime64_0", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime64_5", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime64_3_tz", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
            assertTimestampExtractionFunctionIsNotPushdown("yow", "c_datetime64_9_tz", tableName, "BIGINT '2015', BIGINT '2015', BIGINT '2020', BIGINT '2026', null");
        }
    }

    @Test
    public void testExtractTimezoneIsNotPushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_extract_timezone_function_pushdown_",
                """
                (c_datetime Nullable(DateTime('UTC')),
                c_datetime64_0 Nullable(DateTime64(0, 'Asia/Kathmandu')),
                c_datetime64_5 Nullable(DateTime64(5, 'Europe/Vilnius')),
                c_datetime64_9 Nullable(DateTime64(9, 'Pacific/Apia'))) ENGINE = Log""",
                ImmutableList.of(
                        "'2015-01-01 12:34:56', '2015-01-01 12:34:56', '2015-01-01 12:34:56.12345', '2015-01-01 12:34:56.123456789'",
                        "null, null, null, null"))) {
            String tableName = table.getName();

            assertExtractFunctionIsNotPushdown("timezone(c_datetime)", tableName, "'UTC', null");
            assertExtractFunctionIsNotPushdown("timezone(c_datetime64_0)", tableName, "'+05:45', null");
            assertExtractFunctionIsNotPushdown("timezone(c_datetime64_5)", tableName, "'+02:00', null");
            assertExtractFunctionIsNotPushdown("timezone(c_datetime64_9)", tableName, "'+14:00', null");

            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_hour FROM c_datetime)", tableName, "BIGINT '0', null");
            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_hour FROM c_datetime64_0)", tableName, "BIGINT '5', null");
            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_hour FROM c_datetime64_5)", tableName, "BIGINT '2', null");
            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_hour FROM c_datetime64_9)", tableName, "BIGINT '14', null");

            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_minute FROM c_datetime)", tableName, "BIGINT '0', null");
            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_minute FROM c_datetime64_0)", tableName, "BIGINT '45', null");
            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_minute FROM c_datetime64_5)", tableName, "BIGINT '0', null");
            assertExtractFunctionIsNotPushdown("EXTRACT(timezone_minute FROM c_datetime64_9)", tableName, "BIGINT '0', null");
        }
    }

    private void assertTimestampExtractionFunctionIsPushdown(String function, String columnName, String tableName, String expected)
    {
        assertThat(query("SELECT %s(%s) FROM %s".formatted(function, columnName, tableName)))
                .matches("VALUES %s".formatted(expected))
                .isFullyPushedDown();
        assertThat(query("SELECT EXTRACT(%s FROM %s) FROM %s".formatted(function, columnName, tableName)))
                .matches("VALUES %s".formatted(expected))
                .isFullyPushedDown();
    }

    private void assertTimestampExtractionFunctionIsNotPushdown(String function, String columnName, String tableName, String expected)
    {
        assertThat(query("SELECT %s(%s) FROM %s".formatted(function, columnName, tableName)))
                .matches("VALUES %s".formatted(expected))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT EXTRACT(%s FROM %s) FROM %s".formatted(function, columnName, tableName)))
                .matches("VALUES %s".formatted(expected))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    private void assertExtractFunctionIsPushdown(String function, String tableName, String expected)
    {
        assertThat(query("SELECT %s FROM %s".formatted(function, tableName)))
                .matches("VALUES %s".formatted(expected))
                .isFullyPushedDown();
    }

    private void assertExtractFunctionIsNotPushdown(String function, String tableName, String expected)
    {
        assertThat(query("SELECT %s FROM %s".formatted(function, tableName)))
                .skippingTypesCheck()
                .matches("VALUES %s".formatted(expected))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    @Override // Override because ClickHouse allows SELECT query in update procedure
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertUpdate("CALL system.execute('SELECT 1')");
        assertQueryFails("CALL system.execute('invalid')", "(?s)Failed to execute query.*");
    }

    @Test
    public void testReadTupleType()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_read_tuple",
                "(id Int32, data Tuple(name String, age Int32, active Bool)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, ('Alice', 30, true)), (2, ('Bob', 25, false))");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "(1, CAST(ROW('Alice', 30, true) AS ROW(name varchar, age integer, active boolean))), " +
                            "(2, CAST(ROW('Bob', 25, false) AS ROW(name varchar, age integer, active boolean)))");

            assertThat(query("SELECT data.name FROM " + testTable.getName() + " WHERE data.age > 26"))
                    .matches("VALUES VARCHAR 'Alice'");
        }
    }

    @Test
    public void testReadNestedTupleType()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_read_nested_tuple",
                "(id Int32, info Tuple(person Tuple(name String, age Int32), score Float64)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, (('Alice', 30), 95.5))");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (1, CAST(ROW(CAST(ROW('Alice', 30) AS ROW(name varchar, age integer)), DOUBLE '95.5') AS ROW(person ROW(name varchar, age integer), score double)))");

            assertThat(query("SELECT info.person.name, info.score FROM " + testTable.getName()))
                    .matches("VALUES (VARCHAR 'Alice', DOUBLE '95.5')");
        }
    }

    @Test
    public void testShowCreateTupleColumn()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_describe_tuple",
                "(id Int32, data Tuple(name String, value Int64)) ENGINE=Log")) {
            assertThat(computeScalar("SHOW CREATE TABLE " + testTable.getName()))
                    .isEqualTo(format(
                            """
                            CREATE TABLE clickhouse.%s (
                               id integer NOT NULL,
                               data ROW(name varchar, value bigint) NOT NULL
                            )
                            WITH (
                               engine = 'LOG'
                            )\
                            """,
                            testTable.getName()));
        }
    }

    @Test
    public void testShowCreateTupleUnsupportedColumn()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_describe_tuple",
                "(id Int32, data Tuple(name String, attrs Map(String, String))) ENGINE=Log")) {
            assertThat(computeScalar("SHOW CREATE TABLE " + testTable.getName()))
                    .isEqualTo(format(
                            """
                            CREATE TABLE clickhouse.%s (
                               id integer NOT NULL
                            )
                            WITH (
                               engine = 'LOG'
                            )\
                            """,
                            testTable.getName()));

            Session convertToVarchar = Session.builder(getSession())
                    .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                    .build();
            assertThat(computeScalar(convertToVarchar, "SHOW CREATE TABLE " + testTable.getName()))
                    .isEqualTo(format(
                            """
                            CREATE TABLE clickhouse.%s (
                               id integer NOT NULL,
                               data varchar NOT NULL
                            )
                            WITH (
                               engine = 'LOG'
                            )\
                            """,
                            testTable.getName()));
        }
    }

    @Test
    public void testTupleWithArrayElement()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_with_array",
                "(id Int32, data Tuple(name String, tags Array(String))) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, ('Alice', ['a', 'b']))");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (1, CAST(ROW('Alice', CAST(ARRAY['a', 'b'] AS array(varchar))) AS ROW(name varchar, tags array(varchar))))");
        }
    }

    @Test
    public void testReadArrayType()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_read_array",
                "(id Int32, scores Array(Int32), names Array(String)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, [1, 2, 3], ['Alice', 'Bob']), (2, [42], ['Charlie'])");

            assertThat(query("SELECT * FROM " + testTable.getName() + " ORDER BY id"))
                    .matches("VALUES " +
                            "(1, ARRAY[1, 2, 3], CAST(ARRAY['Alice', 'Bob'] AS array(varchar))), " +
                            "(2, ARRAY[42], CAST(ARRAY['Charlie'] AS array(varchar)))");

            assertThat(query("SELECT id FROM " + testTable.getName() + " WHERE cardinality(scores) > 1"))
                    .matches("VALUES 1");
        }
    }

    @Test
    public void testReadNestedArrayType()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_read_nested_array",
                "(id Int32, matrix Array(Array(Int32))) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, [[1, 2], [3, 4]])");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (1, ARRAY[ARRAY[1, 2], ARRAY[3, 4]])");
        }
    }

    @Test
    public void testShowCreateArrayColumn()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_describe_array",
                "(id Int32, scores Array(Int32), tags Array(String)) ENGINE=Log")) {
            assertThat(computeScalar("SHOW CREATE TABLE " + testTable.getName()))
                    .isEqualTo(format(
                            """
                            CREATE TABLE clickhouse.%s (
                               id integer NOT NULL,
                               scores array(integer) NOT NULL,
                               tags array(varchar) NOT NULL
                            )
                            WITH (
                               engine = 'LOG'
                            )\
                            """,
                            testTable.getName()));
        }
    }

    @Test
    public void testShowCreateArrayUnsupportedColumn()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_describe_array_unsupported",
                "(id Int32, map_col Array(Map(String, String))) ENGINE=Log")) {
            assertThat(computeScalar("SHOW CREATE TABLE " + testTable.getName()))
                    .isEqualTo(format(
                            """
                            CREATE TABLE clickhouse.%s (
                               id integer NOT NULL
                            )
                            WITH (
                               engine = 'LOG'
                            )\
                            """,
                            testTable.getName()));

            Session convertToVarchar = Session.builder(getSession())
                    .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                    .build();
            assertThat(computeScalar(convertToVarchar, "SHOW CREATE TABLE " + testTable.getName()))
                    .isEqualTo(format(
                            """
                            CREATE TABLE clickhouse.%s (
                               id integer NOT NULL,
                               map_col varchar NOT NULL
                            )
                            WITH (
                               engine = 'LOG'
                            )\
                            """,
                            testTable.getName()));
        }
    }

    @Test
    public void testWriteArrayColumnUnsupported()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_write_array",
                "(id Int32, scores Array(Int32)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, [1, 2, 3])");

            assertQueryFails(
                    "INSERT INTO " + testTable.getName() + " SELECT id, scores FROM " + testTable.getName(),
                    "Writing to ClickHouse Array columns is not supported");
        }
    }

    @Test
    public void testTupleWithMapElement()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_tuple_with_map",
                "(id Int32, data Tuple(name String, attrs Map(String, String))) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, ('Alice', {'k': 'v'}))");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES 1");
        }
    }

    @Test
    public void testWriteTupleColumnUnsupported()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "tpch.test_write_tuple",
                "(id Int32, data Tuple(name String, age Int32)) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " VALUES (1, ('Alice', 30))");

            assertQueryFails(
                    "INSERT INTO " + testTable.getName() + " SELECT id, data FROM " + testTable.getName(),
                    "Writing to ClickHouse Tuple columns is not supported");
        }
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // The numeric value depends on file system
        return OptionalInt.of(209);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return clickhouseServer::execute;
    }

    private Map<String, String> getTableProperties(String schemaName, String tableName)
            throws SQLException
    {
        String sql = "SELECT * FROM system.tables WHERE database = ? AND name = ?";
        try (Connection connection = DriverManager.getConnection(clickhouseServer.getJdbcUrl(), clickhouseServer.getUsername(), clickhouseServer.getPassword());
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);

            ResultSet resultSet = preparedStatement.executeQuery();
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            while (resultSet.next()) {
                properties.put(ENGINE_PROPERTY, resultSet.getString("engine"));
                properties.put(ORDER_BY_PROPERTY, resultSet.getString("sorting_key"));
                properties.put(PARTITION_BY_PROPERTY, resultSet.getString("partition_key"));
                properties.put(PRIMARY_KEY_PROPERTY, resultSet.getString("primary_key"));
                properties.put(SAMPLE_BY_PROPERTY, resultSet.getString("sampling_key"));
            }
            return properties.buildOrThrow();
        }
    }

    private DataSetup clickhouseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(onRemoteDatabase()), tableNamePrefix);
    }
}
