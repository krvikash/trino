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
    private static final Pattern FRAGMENT_PATTERN = Pattern.compile("^Fragment ", MULTILINE);

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
                        SELECT count(*), 'first_branch', regionkey FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', nationkey FROM nation GROUP BY nationkey
                       """;

        // no CTE reuse because GROUP BY columns are different
        assertCteReuseNotApplied(query);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (5, 'first_branch', 0),
                    (5, 'first_branch', 1),
                    (5, 'first_branch', 2),
                    (5, 'first_branch', 3),
                    (5, 'first_branch', 4),
                    (1, 'second_branch', 0),
                    (1, 'second_branch', 1),
                    (1, 'second_branch', 2),
                    (1, 'second_branch', 3),
                    (1, 'second_branch', 4),
                    (1, 'second_branch', 5),
                    (1, 'second_branch', 6),
                    (1, 'second_branch', 7),
                    (1, 'second_branch', 8),
                    (1, 'second_branch', 9),
                    (1, 'second_branch', 10),
                    (1, 'second_branch', 11),
                    (1, 'second_branch', 12),
                    (1, 'second_branch', 13),
                    (1, 'second_branch', 14),
                    (1, 'second_branch', 15),
                    (1, 'second_branch', 16),
                    (1, 'second_branch', 17),
                    (1, 'second_branch', 18),
                    (1, 'second_branch', 19),
                    (1, 'second_branch', 20),
                    (1, 'second_branch', 21),
                    (1, 'second_branch', 22),
                    (1, 'second_branch', 23),
                    (1, 'second_branch', 24)
                """);
    }

    @Test
    public void testAggregationWithDifferentGroupingSetsNotUnified()
    {
        String query = """
                        SELECT 'first_branch', regionkey FROM nation GROUP BY GROUPING SETS (regionkey)
                        UNION ALL
                        SELECT 'second_branch', regionkey FROM nation GROUP BY GROUPING SETS (regionkey, ())
                       """;

        // no CTE reuse because GROUPING SETS are different
        assertCteReuseNotApplied(query);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0),
                    ('first_branch', 1),
                    ('first_branch', 2),
                    ('first_branch', 3),
                    ('first_branch', 4),
                    ('second_branch', 0),
                    ('second_branch', 1),
                    ('second_branch', 2),
                    ('second_branch', 3),
                    ('second_branch', 4),
                    ('second_branch', null)
                """);
    }

    @Test
    public void testGlobalAggregationNoFilters()
    {
        String query = """
                        SELECT count(regionkey), 'first_branch' FROM nation
                        UNION ALL
                        SELECT sum(regionkey), 'second_branch' FROM nation
                       """;

        assertCteReuseApplied(query, 3, 2, 1);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (25, 'first_branch'), (50, 'second_branch')");
    }

    @Test
    public void testGroupByNoFilters()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT count(regionkey), 'second_branch', regionkey FROM nation GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (5, 'first_branch', 0),
                    (5, 'first_branch', 1),
                    (5, 'first_branch', 2),
                    (5, 'first_branch', 3),
                    (5, 'first_branch', 4),
                    (5, 'second_branch', 0),
                    (5, 'second_branch', 1),
                    (5, 'second_branch', 2),
                    (5, 'second_branch', 3),
                    (5, 'second_branch', 4)
                """);
    }

    @Test
    public void testFiltersOnGroupingColumns()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation WHERE regionkey = 0 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', regionkey FROM nation WHERE regionkey = 1 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (5, 'first_branch', 0), (5, 'second_branch', 1)");
    }

    @Test
    public void testSameFilterOnNonGroupingColumns()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, "VALUES " +
                "(1, 'first_branch', 0), " +
                "(1, 'second_branch', 0), " +
                "(2, 'first_branch', 1), " +
                "(2, 'second_branch', 1)");
    }

    @Test
    public void testAggregateMasksWithoutWhereClause()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*) FILTER (WHERE nationkey < 10) FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*) FILTER (WHERE nationkey >= 10) FROM nation GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0, 2),
                    ('first_branch', 1, 3),
                    ('first_branch', 2, 2),
                    ('first_branch', 3, 2),
                    ('first_branch', 4, 1),
                    ('second_branch', 0, 3),
                    ('second_branch', 1, 2),
                    ('second_branch', 2, 3),
                    ('second_branch', 3, 3),
                    ('second_branch', 4, 4)
                """);
    }

    @Test
    public void testAggregateMasksWithWhereClauseOnGroupingColumns()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*) FILTER (WHERE nationkey < 5) FROM nation WHERE regionkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*) FILTER (WHERE nationkey >= 5) FROM nation WHERE regionkey < 2 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0, 1),
                    ('first_branch', 1, 3),
                    ('first_branch', 2, 0),
                    ('second_branch', 0, 4),
                    ('second_branch', 1, 2)
                """);
    }

    private void assertCteReuseApplied(String query, int expectedFragmentCountWithoutReuse, int expectedFragmentCountWithReuse, int expectedReusedFragmentId)
    {
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        assertThat(planWithoutCteReuse).isNotEqualTo(planWithCteReuse);
        assertThat(countRegexOccurrences(planWithoutCteReuse, FRAGMENT_PATTERN)).isEqualTo(expectedFragmentCountWithoutReuse);
        assertThat(countRegexOccurrences(planWithCteReuse, FRAGMENT_PATTERN)).isEqualTo(expectedFragmentCountWithReuse);

        // the reused fragment is accessed once per branch that was unified
        int expectedFragmentAccessCount = expectedFragmentCountWithoutReuse - expectedFragmentCountWithReuse + 1;
        assertThat(countRegexOccurrences(planWithCteReuse, "\\QRemoteSource[sourceFragmentIds = [" + expectedReusedFragmentId + "]]\\E"))
                .isEqualTo(expectedFragmentAccessCount);
    }

    private void assertCteReuseNotApplied(String query)
    {
        String explainQuery = "EXPLAIN " + query;

        String planWithoutCteReuse = (String) computeActual(disableCteReuse(), explainQuery).getOnlyValue();
        String planWithCteReuse = (String) computeActual(explainQuery).getOnlyValue();

        // plans should be similar (no CTE reuse)
        assertThat(countRegexOccurrences(planWithoutCteReuse, FRAGMENT_PATTERN))
                .isEqualTo(countRegexOccurrences(planWithCteReuse, FRAGMENT_PATTERN));
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
        return countRegexOccurrences(value, pattern);
    }

    private int countRegexOccurrences(String value, Pattern pattern)
    {
        Matcher matcher = pattern.matcher(value);
        int result = 0;
        while (matcher.find()) {
            result++;
        }
        return result;
    }

    private void assertQueryWithAndWithoutCteReuse(String query, String expectedResults)
    {
        assertQuery(disableCteReuse(), query, expectedResults);
        assertQuery(query, expectedResults);
    }
}
