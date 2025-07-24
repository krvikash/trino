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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCorrelatedJoin
{
    private QueryAssertions assertions;

    @BeforeAll
    public void setup()
    {
        String catalog = "tpch";
        Session session = testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(catalog, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testJoinInCorrelatedJoinInput()
    {
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1) t1(a) JOIN (VALUES 2) t2(b) ON a < b, LATERAL (VALUES 3)"))
                .matches("VALUES (1, 2, 3)");
    }

    @Test
    public void testJoinWithCorrelatedQueriesHavingCrossJoin()
    {
        assertThat(assertions.query("""
                    WITH
                     nation_rates_tbl AS (
                       SELECT
                         map_from_entries(ARRAY_AGG(ROW(nationkey, regionkey))) AS rate_map
                       FROM
                         nation
                     )
                    SELECT
                     (
                       SELECT
                         ARRAY_AGG(
                           ROW(
                             orderkey,
                             quantity,
                             quantity * ELEMENT_AT(
                               (
                                 SELECT
                                   rate_map
                                 FROM
                                   nation_rates_tbl
                               ),
                               nationkey
                             )
                           )
                         )
                       FROM
                         lineitem l
                         LEFT JOIN supplier s
                         ON l.suppkey = s.suppkey
                       WHERE
                         l.orderkey = tbl1.orderkey
                     )
                    FROM
                     lineitem tbl1
                    WHERE
                     tbl1.orderkey = 1 and tbl1.linenumber = 1"""))
                .matches(
                        "VALUES (CAST(ARRAY[ROW(1, 17.0, 0), ROW(1, 36.0, 72.0), ROW(1, 8.0, 32.0), ROW(1, 28.0, 0.0), ROW(1, 24.0, 48.0), ROW(1, 32.0, 32.0)] AS ARRAY(ROW(BIGINT, DOUBLE, DOUBLE))))");

        assertThat(assertions.query("""
                    WITH
                     nation_rates_tbl AS (
                       SELECT
                         map_from_entries(ARRAY_AGG(ROW(nationkey, regionkey))) AS rate_map
                       FROM
                         nation LIMIT 0
                     )
                    SELECT
                     (
                       SELECT
                         ARRAY_AGG(
                           ROW(
                             orderkey,
                             quantity,
                             quantity * ELEMENT_AT(
                               (
                                 SELECT
                                   rate_map
                                 FROM
                                   nation_rates_tbl
                               ),
                               nationkey
                             )
                           )
                         )
                       FROM
                         lineitem l
                         LEFT JOIN supplier s
                         ON l.suppkey = s.suppkey
                       WHERE
                         l.orderkey = tbl1.orderkey
                     )
                    FROM
                     lineitem tbl1
                    WHERE
                     tbl1.orderkey = 1 and tbl1.linenumber = 1"""))
                .matches(
                        "VALUES (CAST(ARRAY[ROW(1, 17.0, NULL), ROW(1, 36.0, NULL), ROW(1, 8.0, NULL), ROW(1, 28.0, NULL), ROW(1, 24.0, NULL), ROW(1, 32.0, NULL)] AS ARRAY(ROW(BIGINT, DOUBLE, DOUBLE))))");
    }

    @Test
    public void testCorrelatedWithUnnestedSubquery()
    {
        // Unnest with LEFT JOIN is only supported as correlated subquery
        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT regionkey, ARRAY_AGG(nationkey) as arr_col FROM nation GROUP BY regionkey)
                        SELECT (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(nation.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (nationkey) LEFT JOIN nation ON nation.nationkey = inner_query.nationkey) AS nation_name FROM table_with_array as outer_query
                        """))
                .matches(
                        """
                                VALUES
                                  (CAST(ARRAY['EGYPT', 'IRAN', 'IRAQ', 'JORDAN', 'SAUDI ARABIA'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY['CHINA', 'INDIA', 'INDONESIA', 'JAPAN', 'VIETNAM'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY['ALGERIA', 'ETHIOPIA', 'KENYA', 'MOROCCO', 'MOZAMBIQUE'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY['ARGENTINA', 'BRAZIL', 'CANADA', 'PERU', 'UNITED STATES'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY['FRANCE', 'GERMANY', 'ROMANIA', 'RUSSIA', 'UNITED KINGDOM'] AS ARRAY(VARCHAR)))""");

        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT regionkey, ARRAY_AGG(nationkey) as arr_col FROM nation GROUP BY regionkey)
                        SELECT (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(nation.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (nationkey) LEFT JOIN nation ON nation.nationkey = inner_query.nationkey AND nation.regionkey % 2 = 0) AS nation_name FROM table_with_array as outer_query
                        """))
                .matches(
                        """
                                VALUES
                                  (CAST(ARRAY['EGYPT', 'IRAN', 'IRAQ', 'JORDAN', 'SAUDI ARABIA'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY['CHINA', 'INDIA', 'INDONESIA', 'JAPAN', 'VIETNAM'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY['ALGERIA', 'ETHIOPIA', 'KENYA', 'MOROCCO', 'MOZAMBIQUE'] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY[NULL, NULL, NULL, NULL, NULL] AS ARRAY(VARCHAR))),
                                  (CAST(ARRAY[NULL, NULL, NULL, NULL, NULL] AS ARRAY(VARCHAR)))""");


        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT * FROM (values (1, ARRAY[1, 1, 2]), (1, ARRAY[2, 3]), (2, ARRAY[3, 4]), (3, ARRAY[]), (1, ARRAY[1, 4, 5])) as table_with_array(key, arr_col)),
                             table_with_multiple_names AS (SELECT * FROM (VALUES (1, 'Chennai'), (1, 'Coimbatore'), (1, 'Udupi'), (2, 'Bengaluru'), (2, 'Mumbai'), (2, 'Hyderabad'), (3, 'Kolkata'), (4, 'New Delhi')) as city_names(key, name))
                        SELECT key, (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(table_with_multiple_names.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (citykey) LEFT JOIN table_with_multiple_names ON table_with_multiple_names.key = inner_query.citykey) AS nation_name FROM table_with_array as outer_query ORDER BY outer_query.key
                        """))
                .matches(
                        """
                                VALUES
                                  (1, CAST(ARRAY['Chennai', 'Coimbatore', 'New Delhi', 'Udupi', NULL] AS ARRAY(VARCHAR))),
                                  (1, CAST(ARRAY['Bengaluru', 'Hyderabad', 'Kolkata', 'Mumbai'] AS ARRAY(VARCHAR))),
                                  (1, CAST(ARRAY['Bengaluru', 'Chennai', 'Chennai', 'Coimbatore', 'Coimbatore', 'Hyderabad', 'Mumbai', 'Udupi', 'Udupi'] AS ARRAY(VARCHAR))),
                                  (2, CAST(ARRAY['Kolkata', 'New Delhi'] AS ARRAY(VARCHAR))),
                                  (3, NULL)""");


        // Correlated queries with filters and UNNEST are not supported
        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT regionkey, ARRAY_AGG(nationkey) as arr_col FROM nation GROUP BY regionkey)
                        SELECT (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(nation.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (nationkey) LEFT JOIN nation ON nation.nationkey = inner_query.nationkey WHERE nation.regionkey % 2 = 0) AS nation_name FROM table_with_array as outer_query
                        """))
                .failure()
                .hasMessageContaining("Given correlated subquery is not supported");


        // Unnest with INNER JOIN is not supported as correlated subquery
        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT regionkey, ARRAY_AGG(nationkey) as arr_col FROM nation GROUP BY regionkey)
                        SELECT (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(nation.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (nationkey) INNER JOIN nation ON nation.nationkey = inner_query.nationkey) AS nation_name FROM table_with_array as outer_query
                        """))
                .failure()
                .hasMessageContaining("Given correlated subquery is not supported");

        // Unnest with RIGHT JOIN is not supported as correlated subquery
        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT regionkey, ARRAY_AGG(nationkey) as arr_col FROM nation GROUP BY regionkey)
                        SELECT (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(nation.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (nationkey) RIGHT JOIN nation ON nation.nationkey = inner_query.nationkey) AS nation_name FROM table_with_array as outer_query
                        """))
                .failure()
                .hasMessageContaining("Given correlated subquery is not supported");

        // Unnest with FULL JOIN is not supported as correlated subquery
        assertThat(assertions.query(
                """
                        WITH table_with_array AS (SELECT regionkey, ARRAY_AGG(nationkey) as arr_col FROM nation GROUP BY regionkey)
                        SELECT (
                            SELECT ARRAY_SORT(ARRAY_AGG(CAST(nation.name AS VARCHAR))) FROM UNNEST(outer_query.arr_col) AS inner_query (nationkey) FULL JOIN nation ON nation.nationkey = inner_query.nationkey) AS nation_name FROM table_with_array as outer_query
                        """))
                .failure()
                .hasMessageContaining("Given correlated subquery is not supported");
    }
}
