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
package com.starburstdata.plugin.kdb;

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

class TestKdbConnectorTest
        extends BaseConnectorTest
{
    private KdbClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        KdbContainer server = closeAfterClass(new KdbContainer());
        client = server.client();
        return KdbQueryRunner.builder(server)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_INSERT,
                 SUPPORTS_DELETE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_ROW_LEVEL_UPDATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_ARRAY,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE,
                 SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_OR_REPLACE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ADD_FIELD,
                 SUPPORTS_ADD_FIELD_IN_ARRAY,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_DROP_FIELD,
                 SUPPORTS_DROP_FIELD_IN_ARRAY,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_FIELD,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_SET_FIELD_TYPE,
                 SUPPORTS_SET_FIELD_TYPE_IN_ARRAY,
                 SUPPORTS_SET_FIELD_TYPE_IN_MAP,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_COMMENT_ON_VIEW,
                 SUPPORTS_COMMENT_ON_VIEW_COLUMN,
                 SUPPORTS_REFRESH_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW_WHEN_STALE,
                 SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW,
                 SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS,
                 SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_SET_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DROP_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_CREATE_FUNCTION,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_CANCELLATION,
                 SUPPORTS_MULTI_STATEMENT_WRITES,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_REPORTING_WRITTEN_BYTES -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
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

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(
                        """
                        CREATE TABLE kdb.default.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar,
                           totalprice double,
                           orderdate date,
                           orderpriority varchar,
                           clerk varchar,
                           shippriority integer,
                           comment varchar
                        )\
                        """);
    }

    @Test
    void testUnsupportedColumnType()
    {
        client.execute("test_unsupported_type: flip `col1`col2`col3 ! (`long$(); ([] x:`long$()); `long$())");
        client.execute("`test_unsupported_type insert (1; ([] x: enlist 999); 10)");

        assertThat(query("DESC test_unsupported_type")).skippingTypesCheck()
                .matches("VALUES ('col1', 'bigint', '', ''), ('col3', 'bigint', '', '')");

        assertThat(query("TABLE test_unsupported_type"))
                .matches("VALUES (BIGINT '1', BIGINT '10')");
    }
}
