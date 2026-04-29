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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Program;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.sql.query.QueryAssertions.ProgramAssert.newProgramAssert;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGroupIdWithCteReuse
        extends AbstractTestQueryFramework
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
    public void testIdenticalGroupingSetsMerge()
    {
        String query =
                """
                SELECT 'a', regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT 'b', regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testSameGroupingSetsDifferentAggregationArgumentsMerge()
    {
        // both aggregates produce bigint (compatible UNION types) but reference different input columns;
        // the merged GroupId's aggregation-argument selector is the union of both branches' arguments
        String query =
                """
                SELECT regionkey, nationkey, count(name) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(comment) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testGroupingFunctionMerges()
    {
        // GROUPING(...) consumes the group_id column; identical grouping sets still merge
        String query =
                """
                SELECT regionkey, nationkey, GROUPING(regionkey, nationkey), count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, GROUPING(regionkey, nationkey), count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testIdenticalCubeMerges()
    {
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY CUBE (regionkey, nationkey)
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY CUBE (regionkey, nationkey)
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testIdenticalRollupMerges()
    {
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY ROLLUP (regionkey, nationkey)
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY ROLLUP (regionkey, nationkey)
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testResidualFilterOnNonGroupingColumnMerges()
    {
        // the filtered column is not part of the grouping sets, so it becomes a deferred residual predicate
        String query =
                """
                SELECT regionkey, count(*) FROM nation WHERE nationkey < 12 GROUP BY GROUPING SETS ((regionkey, name), (regionkey))
                UNION ALL
                SELECT regionkey, count(*) FROM nation WHERE nationkey >= 12 GROUP BY GROUPING SETS ((regionkey, name), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testResidualFilterOnGroupingColumnMerges()
    {
        // the filtered column (nationkey) IS a grouping column, so for the (regionkey) set its GroupId output is NULL;
        // the deferred predicate must rebase onto the non-NULL aggregation-argument slot. If the filter is wrongly applied
        // to the NULL GroupId, the result would be incorrect.
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey < 12 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey >= 12 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testEmptySourceMerges()
    {
        // both branches filter out every row; identical grouping sets still merge and the result is empty
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey < 0 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey < 0 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testThreeIdenticalBranchesMergeIntoOne()
    {
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey < 8 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey >= 8 AND nationkey < 16 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation WHERE nationkey >= 16 GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(3);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(1);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testThreeBranchesPartialMerge()
    {
        // first two share grouping sets and merge; the third has different grouping sets and is left alone
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (nationkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(3);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(2)
                // first two branches share ((regionkey, nationkey), (regionkey)) and merge into one GroupId;
                // the third over ((regionkey, nationkey), (nationkey)) stays standalone
                .expectedGroupIdGroupingSets(ImmutableList.of(
                        ImmutableList.of(ImmutableList.of(0, 1), ImmutableList.of(0)),
                        ImmutableList.of(ImmutableList.of(0, 1), ImmutableList.of(1))));
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testDifferentGroupingSetsDoNotMerge()
    {
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (nationkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertSameResultsWithAndWithoutCteReuse(query);

        // same grouping-set shape but over different columns -> grouping-columns selectors are not equivalent.
        // only the common column (regionkey) and the aggregate are projected so the UNION types line up; the
        // differing grouping column lives solely in the GROUP BY.
        query =
                """
                SELECT regionkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, name), (regionkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testReorderedGroupingSetsDoNotMerge()
    {
        // same set of grouping sets but listed in a different order; grouping-set equality is order-sensitive
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey, nationkey), (regionkey))
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((regionkey), (regionkey, nationkey))
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    @Test
    public void testCubeVersusRollupDoNotMerge()
    {
        // CUBE and ROLLUP over the same columns expand to different sets of grouping sets
        String query =
                """
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY CUBE (regionkey, nationkey)
                UNION ALL
                SELECT regionkey, nationkey, count(*) FROM nation GROUP BY ROLLUP (regionkey, nationkey)
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedGroupIdOperationCount(2);
        assertSameResultsWithAndWithoutCteReuse(query);
    }

    private void assertSameResultsWithAndWithoutCteReuse(String query)
    {
        MaterializedResult withoutReuse = computeActual(getSessionForDisabledCteReuse(), query);
        MaterializedResult withReuse = computeActual(getSession(), query);
        assertThat(withReuse.getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(withoutReuse.getMaterializedRows());
    }

    private Program getProgramForDisabledCteReuse(String query)
    {
        return getQueryRunner().executeWithPlan(getSessionForDisabledCteReuse(), query)
                .queryPlan()
                .map(plan -> ProgramBuilder.buildProgram(plan.getRoot())).orElseThrow();
    }

    private Program getProgramForEnabledCteReuse(String query)
    {
        return getQueryRunner().executeWithPlan(getSession(), query).program().orElseThrow();
    }

    private Session getSessionForDisabledCteReuse()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("reuse_common_subqueries", "false")
                .build();
    }
}
