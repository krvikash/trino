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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.util.List;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
public class TestNonShardedOpenSearchLatestConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected static final String DEFAULT_LATEST_IMAGE = "opensearchproject/opensearch:latest";
    protected OpenSearchServer opensearch;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        opensearch = new OpenSearchServer(DEFAULT_LATEST_IMAGE, false, ImmutableMap.of());

        return OpenSearchQueryRunner.builder(opensearch.getAddress())
                .setInitialTables(ImmutableList.of(NATION, REGION, ORDERS))
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            case SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @AfterAll
    public final void destroy()
            throws IOException
    {
        opensearch.close();
        opensearch = null;
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(format("CREATE TABLE %s.tpch.orders (\n", catalogName) +
                        "   clerk varchar,\n" +
                        "   comment varchar,\n" +
                        "   custkey bigint,\n" +
                        "   orderdate timestamp(3),\n" +
                        "   orderkey bigint,\n" +
                        "   orderpriority varchar,\n" +
                        "   orderstatus varchar,\n" +
                        "   shippriority bigint,\n" +
                        "   totalprice real\n" +
                        ")");
    }

    @Test
    public void testQueryTableFunction()
    {
        String catalogName = getSession().getCatalog().orElseThrow();

        // select single record
        assertThat(query("SELECT json_query(result, 'lax $[0][0].hits.hits._source') " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"query\": {\"match\": {\"name\": \"ALGERIA\"}}}')) t(result)"))
                .matches("VALUES VARCHAR '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // just nested query
        assertThat(query("SELECT json_query(result, 'lax $[0][0].hits.hits._source') " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"match\": {\"name\": \"ALGERIA\"}}')) t(result)"))
                .matches("VALUES VARCHAR '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // parameters
        Session session = Session.builder(getSession())
                .addPreparedStatement(
                        "my_query",
                        format("SELECT json_query(result, 'lax $[0][0].hits.hits._source') FROM TABLE(%s.system.raw_query(schema => ?, index => ?, query => ?))", catalogName))
                .build();
        assertThat(query(session, "EXECUTE my_query USING 'tpch', 'nation', '{\"query\": {\"match\": {\"name\": \"ALGERIA\"}}}'"))
                .matches("VALUES VARCHAR '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // select multiple records by range. Use array wrapper to wrap multiple results
        assertThat(query("SELECT array_sort(CAST(json_parse(json_query(result, 'lax $[0][0].hits.hits._source.name' WITH ARRAY WRAPPER)) AS array(varchar))) " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"query\": {\"range\": {\"nationkey\": {\"gte\": 0,\"lte\": 3}}}}')) t(result)"))
                .matches("VALUES CAST(ARRAY['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA'] AS ARRAY(VARCHAR))");

        List<Integer> querySizeOptions = List.of(1, 2, 5, 10, 1000, 10000);

        for (Integer size : querySizeOptions) {
            assertThat(query("SELECT array_sort(array_agg(name)) " +
                    "FROM (SELECT name " +
                    format("FROM TABLE(%s.system.raw_query(", catalogName) +
                    "schema => 'tpch', " +
                    "index => 'orders', " +
                    "query => '{\"size\": %s,\"query\": {\"range\": {\"orderkey\": {\"gte\": 0,\"lte\": 4}}}}')) t(result), ".formatted(size) +
                    "UNNEST(CAST(json_parse(json_query(result, 'lax $[*][*].hits.hits[*]._source.clerk' WITH ARRAY WRAPPER)) AS array(varchar))) AS t(name))"))
                    .matches("VALUES CAST(ARRAY['Clerk#000000124', 'Clerk#000000880', 'Clerk#000000951', 'Clerk#000000955'] AS ARRAY(VARCHAR))");
        }

        // invalid scroll size
        assertThat(query("SELECT array_sort(array_agg(name)) " +
                "FROM (SELECT name " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"size\": 100000,\"query\": {\"range\": {\"nationkey\": {\"gte\": 0,\"lte\": 3}}}}')) t(result), " +
                "UNNEST(CAST(json_parse(json_query(result, 'lax $[*][*].hits.hits[*]._source.name' WITH ARRAY WRAPPER)) AS array(varchar))) AS t(name))"))
                .failure().hasMessageContaining("Batch size is too large");

        // use aggregations
        @Language("JSON")
        String query = "{\n" +
                "    \"size\": 0,\n" +
                "    \"aggs\" : {\n" +
                "        \"max_orderkey\" : { \"max\" : { \"field\" : \"orderkey\" } },\n" +
                "        \"sum_orderkey\" : { \"sum\" : { \"field\" : \"orderkey\" } }\n" +
                "    }\n" +
                "}";

        assertThat(query(format("WITH data(r) AS (" +
                "   SELECT CAST(json_parse(result) AS ROW(aggregations ROW(max_orderkey ROW(value BIGINT), sum_orderkey ROW(value BIGINT)))) " +
                "   FROM TABLE(%s.system.raw_query(" +
                "                        schema => 'tpch', " +
                "                        index => 'orders', " +
                "                        query => '%s'))) " +
                "SELECT r.aggregations.max_orderkey.value, r.aggregations.sum_orderkey.value " +
                "FROM data", catalogName, query)))
                .matches("VALUES (BIGINT '60000', BIGINT '449872500')");

        // no matches
        assertThat(query("SELECT json_query(result, 'lax $[0][0].hits.hits') " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"query\": {\"match\": {\"name\": \"UTOPIA\"}}}')) t(result)"))
                .matches("VALUES VARCHAR '[]'");

        // syntax error
        assertThat(query("SELECT * " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => 'wrong syntax')) t(result)"))
                .failure().hasMessageContaining("Unrecognized token 'wrong': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");
    }
}
