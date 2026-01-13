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
package io.trino.tests;

import io.trino.Session;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.MULTILINE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationsWithCteReuse
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder()
                .addExtraProperty("optimizer.reuse-common-subqueries", "true")
                .withExchange("filesystem")
                .build();
    }

    @Test
    public void testAggregationWithDifferentGroupByNotUnified()
    {
        String query = """
                        SELECT count(*) FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT count(*) FROM nation GROUP BY nationkey
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        // plans should be similar (no CTE reuse) because GROUP BY columns are different
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment "))
                .isEqualTo(countRegexOccurrences(planWithCteReuse, "^Fragment "));
        // results are ok
        assertQuery(query, "SELECT 5 FROM SYSTEM_RANGE(1, 5) UNION ALL SELECT 1 FROM SYSTEM_RANGE(1, 25)");
    }

    @Test
    public void testAggregationWithDifferentGroupingSetsNotUnified()
    {
        String query = """
                        SELECT regionkey FROM nation GROUP BY GROUPING SETS (regionkey)
                        UNION ALL
                        SELECT regionkey FROM nation GROUP BY GROUPING SETS (regionkey, ())
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        // plans should be similar (no CTE reuse) because GROUPING SETS are different
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment "))
                .isEqualTo(countRegexOccurrences(planWithCteReuse, "^Fragment "));
        // results are ok
        assertQuery(query, "VALUES (0), (0), (1), (1), (2), (2), (3), (3), (4), (4), (null)");
    }

    @Test
    public void testGlobalAggregationNoFilters()
    {
        String query = """
                        SELECT count(regionkey) FROM nation
                        UNION ALL
                        SELECT sum(regionkey) FROM nation
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(3);
        // plan with CTE reuse has just 2 fragments and leaf fragment is accessed twice
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(2);
        assertThat(countRegexOccurrences(planWithCteReuse, "\\QRemoteSource[sourceFragmentIds = [1]]\\E")).isEqualTo(2);
        // results are ok
        assertQuery(query, "VALUES (25), (50)");
    }

    @Test
    public void testGroupByNoFilters()
    {
        String query = """
                        SELECT count(*) FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT count(regionkey) FROM nation GROUP BY regionkey
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(5);
        // plan with CTE reuse has just 4 fragments and leaf fragment is accessed twice
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(4);
        assertThat(countRegexOccurrences(planWithCteReuse, "\\QRemoteSource[sourceFragmentIds = [2]]\\E")).isEqualTo(2);
        // results are ok
        assertQuery(query, "SELECT 5 FROM SYSTEM_RANGE(1, 10)");
    }

    @Test
    public void testFiltersOnGroupingColumns()
    {
        String query = """
                        SELECT count(*) FROM nation WHERE regionkey = 0 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*) FROM nation WHERE regionkey = 1 GROUP BY regionkey
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(5);
        // plan with CTE reuse has just 4 fragments and leaf fragment is accessed twice
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(4);
        assertThat(countRegexOccurrences(planWithCteReuse, "\\QRemoteSource[sourceFragmentIds = [2]]\\E")).isEqualTo(2);
        // results are ok
        assertQuery(query, "VALUES (5), (5)");
    }

    @Test
    public void testSameFilterOnNonGroupingColumns()
    {
        String query = """
                        SELECT count(*) AS value FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*) AS value FROM nation WHERE nationkey < 3 GROUP BY regionkey
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(5);
        // plan with CTE reuse has just 4 fragments and leaf fragment is accessed twice
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(4);
        assertThat(countRegexOccurrences(planWithCteReuse, "\\QRemoteSource[sourceFragmentIds = [2]]\\E")).isEqualTo(2);
        // results are ok
        assertQuery(query, "VALUES (1), (1), (2), (2)");
    }

    @Test
    public void testAggregateMasksWithoutWhereClause()
    {
        String query = """
                        SELECT regionkey, count(*) FILTER (WHERE nationkey < 10) FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT regionkey, count(*) FILTER (WHERE nationkey >= 10) FROM nation GROUP BY regionkey
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(5);
        // plan with CTE reuse has just 4 fragments and leaf fragment is accessed twice
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(4);
        // results are ok
        assertQuery(query, "VALUES (0, 2), (0, 3), (1, 2), (1, 3), (2, 2), (2, 3), (3, 2), (3, 3), (4, 4), (4, 1)");
    }

    @Test
    public void testAggregateMasksWithWhereClauseOnGroupingColumns()
    {
        String query = """
                        SELECT regionkey, count(*) FILTER (WHERE nationkey < 5) FROM nation WHERE regionkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT regionkey, count(*) FILTER (WHERE nationkey >= 5) FROM nation WHERE regionkey < 2 GROUP BY regionkey
                       """;
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, "^Fragment ")).isEqualTo(5);
        // plan with CTE reuse has just 4 fragments and leaf fragment is accessed twice
        assertThat(countRegexOccurrences(planWithCteReuse, "^Fragment ")).isEqualTo(4);
        // results are ok
        assertQuery(query, "VALUES (0, 1), (0, 4), (1, 2), (1, 3), (2, 0)");
    }

    private Session disableCteReuse()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("reuse_common_subqueries", "false")
                .build();
    }

    private int countRegexOccurrences(String value, String regex)
    {
        Pattern pattern = Pattern.compile(regex, MULTILINE);
        Matcher matcher = pattern.matcher(value);
        int result = 0;
        while (matcher.find()) {
            result++;
        }
        return result;
    }
}
