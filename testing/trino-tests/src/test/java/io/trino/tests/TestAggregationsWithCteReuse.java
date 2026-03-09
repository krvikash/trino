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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
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
        QueryRunner queryRunner = TpchQueryRunner.builder()
                .addExtraProperty("optimizer.reuse-common-subqueries", "true")
                .withExchange("filesystem")
                .build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());

        return queryRunner;
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
    public void testFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey > 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (1, 'first_branch', 0),
                    (2, 'first_branch', 1),
                    (4, 'second_branch', 0),
                    (2, 'second_branch', 1),
                    (5, 'second_branch', 2),
                    (5, 'second_branch', 3),
                    (5, 'second_branch', 4)
                """);
    }

    @Test
    public void testWithAndWithoutFilterOnNonGroupingColumn()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (5, 'first_branch', 0),
                    (5, 'first_branch', 1),
                    (5, 'first_branch', 2),
                    (5, 'first_branch', 3),
                    (5, 'first_branch', 4),
                    (1, 'second_branch', 0),
                    (2, 'second_branch', 1)
                """);
    }

    @Test
    public void testCountColumnWithFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT count(nationkey), 'first_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT count(nationkey), 'second_branch', regionkey FROM nation WHERE nationkey > 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (1, 'first_branch', 0),
                    (2, 'first_branch', 1),
                    (4, 'second_branch', 0),
                    (2, 'second_branch', 1),
                    (5, 'second_branch', 2),
                    (5, 'second_branch', 3),
                    (5, 'second_branch', 4)
                """);
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

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (1, 'first_branch', 0),
                    (2, 'first_branch', 1),
                    (1, 'second_branch', 0),
                    (2, 'second_branch', 1)
                """);
    }

    @Test
    public void testSameAndDifferentFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'third_branch', regionkey FROM nation WHERE nationkey > 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 7, 5, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (1, 'first_branch', 0),
                    (2, 'first_branch', 1),
                    (1, 'second_branch', 0),
                    (2, 'second_branch', 1),
                    (2, 'third_branch', 1),
                    (4, 'third_branch', 0),
                    (5, 'third_branch', 2),
                    (5, 'third_branch', 3),
                    (5, 'third_branch', 4)
                """);
    }

    @Test
    public void testSubsetFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*) FROM nation WHERE nationkey < 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0, 1),
                    ('first_branch', 1, 3),
                    ('first_branch', 4, 1),
                    ('second_branch', 0, 1),
                    ('second_branch', 1, 2)
                """);
    }

    @Test
    public void testFiltersOnGroupingAndNonGroupingColumns()
    {
        String query = """
                        SELECT count(*), 'first_branch', regionkey FROM nation WHERE nationkey < 3 and regionkey = 0 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey > 3 and regionkey = 1 GROUP BY regionkey
                        UNION ALL
                        SELECT count(*), 'third_branch', regionkey FROM nation WHERE nationkey < 3 and regionkey = 2 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 7, 5, 2);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (1, 'first_branch', 0), (2, 'second_branch', 1)");
    }

    @Test
    public void testGlobalAggregationsWithFilters()
    {
        String query = """
                        SELECT count(*), 'first_branch' FROM nation WHERE nationkey > 0
                        UNION ALL
                        SELECT count(*), 'second_branch' FROM nation WHERE nationkey = 0
                       """;

        assertCteReuseApplied(query, 3, 2, 1);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (24, 'first_branch'), (1, 'second_branch')");
    }

    @Test
    public void testGlobalAggregationsWithOneEmptyPartition()
    {
        String query = """
                        SELECT count(*), 'first_branch' FROM nation WHERE nationkey > 0
                        UNION ALL
                        SELECT count(*), 'second_branch' FROM nation WHERE nationkey < 0
                       """;

        assertCteReuseApplied(query, 3, 2, 1);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (24, 'first_branch'), (0, 'second_branch')");
    }

    @Test
    public void testGroupByWithOneEmptyPartition()
    {
        String query = """
                    SELECT count(*), 'first_branch', regionkey FROM nation WHERE nationkey > 0 GROUP BY regionkey
                    UNION ALL
                    SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey < 0 GROUP BY regionkey
                   """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
            VALUES
                (4, 'first_branch', 0),
                (5, 'first_branch', 1),
                (5, 'first_branch', 2),
                (5, 'first_branch', 3),
                (5, 'first_branch', 4)
            """);
    }

    @Test
    public void testGlobalAggregationsWithBothEmptyPartitions()
    {
        String query = """
                        SELECT count(*), 'first_branch' FROM nation WHERE nationkey < 0
                        UNION ALL
                        SELECT count(*), 'second_branch' FROM nation WHERE nationkey > 1000
                       """;

        assertCteReuseApplied(query, 3, 2, 1);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (0, 'first_branch'), (0, 'second_branch')");
    }

    @Test
    public void testGroupByWithBothEmptyPartitions()
    {
        String query = """
                    SELECT count(*), 'first_branch', regionkey FROM nation WHERE nationkey < 0 GROUP BY regionkey
                    UNION ALL
                    SELECT count(*), 'second_branch', regionkey FROM nation WHERE nationkey > 1000 GROUP BY regionkey
                   """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, "SELECT 1 WHERE false");
    }

    @Test
    public void testGlobalAggregationsWithAndWithoutFilter()
    {
        String query = """
                        SELECT count(*), 'first_branch' FROM nation
                        UNION ALL
                        SELECT count(*), 'second_branch' FROM nation WHERE nationkey = 0
                       """;

        assertCteReuseApplied(query, 3, 2, 1);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (25, 'first_branch'), (1, 'second_branch')");
    }

    @Test
    public void testMultipleAggregatesWithFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*), sum(nationkey) FROM nation WHERE nationkey < 10 GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*), max(nationkey) FROM nation WHERE nationkey > 5 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0, 2, 5),
                    ('first_branch', 1, 3, 6),
                    ('first_branch', 2, 2, 17),
                    ('first_branch', 3, 2, 13),
                    ('first_branch', 4, 1, 4),
                    ('second_branch', 0, 3, 16),
                    ('second_branch', 1, 2, 24),
                    ('second_branch', 2, 5, 21),
                    ('second_branch', 3, 5, 23),
                    ('second_branch', 4, 4, 20)
                """);
    }

    @Test
    public void testThreeBranchesWithFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*) FROM nation WHERE nationkey BETWEEN 5 AND 15 GROUP BY regionkey
                        UNION ALL
                        SELECT 'third_branch', regionkey, count(*) FROM nation WHERE nationkey > 15 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 7, 5, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0, 1),
                    ('first_branch', 1, 3),
                    ('first_branch', 4, 1),
                    ('second_branch', 0, 3),
                    ('second_branch', 2, 3),
                    ('second_branch', 3, 2),
                    ('second_branch', 4, 3),
                    ('third_branch', 0, 1),
                    ('third_branch', 1, 2),
                    ('third_branch', 2, 2),
                    ('third_branch', 3, 3),
                    ('third_branch', 4, 1)
                """);
    }

    @Test
    public void testMultipleGroupingKeys()
    {
        String query = """
                        SELECT 'first_branch', regionkey, MOD(nationkey, 2) as parity, count(*) FROM nation WHERE nationkey < 3 GROUP BY regionkey, MOD(nationkey, 2)
                        UNION ALL
                        SELECT 'second_branch', regionkey, MOD(nationkey, 2) as parity, count(*) FROM nation WHERE nationkey >= 12 GROUP BY regionkey, MOD(nationkey, 2)
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 0, 0, 1),
                    ('first_branch', 1, 0, 1),
                    ('first_branch', 1, 1, 1),
                    ('second_branch', 0, 0, 2),
                    ('second_branch', 0, 1, 1),
                    ('second_branch', 1, 0, 1),
                    ('second_branch', 1, 1, 1),
                    ('second_branch', 2, 0, 2),
                    ('second_branch', 2, 1, 1),
                    ('second_branch', 3, 0, 1),
                    ('second_branch', 3, 1, 2),
                    ('second_branch', 4, 0, 1),
                    ('second_branch', 4, 1, 1)
                """);
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

    @Test
    public void testAggregateMasksWithWhereClauseOnNonGroupingColumns()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*) FILTER (WHERE nationkey < 5) FROM nation WHERE nationkey > 2 GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*) FILTER (WHERE nationkey > 10) FROM nation WHERE nationkey < 15 GROUP BY regionkey
                       """;

        assertCteReuseNotApplied(query);
    }

    @Test
    public void testMultipleAggregateMasksWithWhereClauseOnNonGroupingColumns()
    {
        String query = """
                        SELECT 'first_branch', regionkey, count(*) FILTER (WHERE nationkey < 5), count(*) FILTER (WHERE nationkey < 4) FROM nation WHERE nationkey > 2 GROUP BY regionkey
                        UNION ALL
                        SELECT 'second_branch', regionkey, count(*) FILTER (WHERE nationkey > 10), count(*) FILTER (WHERE nationkey > 12) FROM nation WHERE nationkey < 15 GROUP BY regionkey
                       """;

        assertCteReuseNotApplied(query);
    }

    @Test
    public void testGlobalAggregationWithMasksAndFilters()
    {
        String query = """
                        SELECT count(*) FILTER (WHERE nationkey < 10), 'first_branch' FROM nation WHERE regionkey < 3
                        UNION ALL
                        SELECT count(*) FILTER (WHERE nationkey >= 10), 'second_branch' FROM nation WHERE regionkey >= 2
                       """;

        assertCteReuseNotApplied(query);
    }

    @Test
    public void testAggregateMaskOnOneOutOfTwoBranchesWithWhereClauseOnNonGroupingColumns()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_aggregate_mask_two_branches",
                "(b boolean, c1 int, c2 int)",
                List.of(
                        "true, 1, 1",
                        "true, 5, 2",
                        "false, 1, 1",
                        "false, 5, 2"))) {
            // use direct column reference (not expression) in aggregate filter to avoid creating an extra projection in one branch.
            // Otherwise, this branch will enter identifySingleGroupMergeCandidates() with a project as the next operation, while the other
            // branch's next operation is an aggregation. This causes the branch with the aggregate filter to be skipped before the grouping logic.
            String query = String.format("""
                     SELECT 'first_branch', c2, count(*) FILTER (WHERE b) FROM %s WHERE c1 > 1 GROUP BY c2
                     UNION ALL
                     SELECT 'second_branch', c2, count(*) FROM %s WHERE c1 < 5 GROUP BY c2
                    """, table.getName(), table.getName());

            assertCteReuseNotApplied(query);
        }
    }

    @Test
    public void testAggregateMaskOnOneOutOfThreeBranchesWithWhereClauseOnNonGroupingColumns()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_aggregate_mask_three_branches",
                "(b boolean, c1 int, c2 int)",
                List.of(
                        "true, 1, 1",
                        "true, 2, 2",
                        "false, 3, 1",
                        "false, 4, 2"))) {
            // use direct column reference (not expression) in aggregate filter to avoid creating an extra projection in one branch.
            // Otherwise, this branch will enter identifySingleGroupMergeCandidates() with a project as the next operation, while all other
            // branches' next operation is an aggregation. This causes the branch with the aggregate filter to be skipped before the grouping logic.
            String query = String.format("""
                     SELECT 'first_branch', c2, count(*) FILTER (WHERE b) FROM %s WHERE c1 > 1 GROUP BY c2
                     UNION ALL
                     SELECT 'second_branch', c2, count(*) FROM %s WHERE c1 < 4 GROUP BY c2
                     UNION ALL
                     SELECT 'third_branch', c2, count(*) FROM %s WHERE c1 > 2 GROUP BY c2
                    """, table.getName(), table.getName(), table.getName());

            // only the branches without masks should be merged
            assertCteReuseApplied(query, 7, 6, 4);

            assertQueryWithAndWithoutCteReuse(query, """
                    VALUES
                        ('first_branch', 1, 0),
                        ('first_branch', 2, 1),
                        ('second_branch', 1, 2),
                        ('second_branch', 2, 1),
                        ('third_branch', 1, 1),
                        ('third_branch', 2, 1)
                    """);
        }
    }

    @Test
    public void testGroupByWithoutAggregates()
    {
        String query = """
                SELECT 'first_branch', regionkey FROM nation WHERE nationkey > 22 GROUP BY regionkey
                UNION ALL
                SELECT 'second_branch', regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    ('first_branch', 1),
                    ('first_branch', 3),
                    ('second_branch', 0),
                    ('second_branch', 1)
                """);
    }

    @Test
    public void testFiltersOnNonGroupingColumnsAndNullValues()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_filters_null_values",
                "(groupkey int, c1 int, c2 int, c3 int)",
                List.of(
                        "0, null, null, 0",
                        "1, null, 1, 0",
                        "2, 1, null, 1",
                        "3, 1, 1, 1"))) {
            String query = String.format("""
                     SELECT groupkey, count_if(c1 IS NULL), count_if(c2 IS NULL) FROM %s WHERE c3 = 0 GROUP BY groupkey
                     UNION ALL
                     SELECT groupkey, count_if(c1 IS NULL), count_if(c2 IS NULL) FROM %s WHERE c3 = 1 GROUP BY groupkey
                    """, table.getName(), table.getName());

            assertCteReuseApplied(query, 5, 4, 2);

            assertQueryWithAndWithoutCteReuse(query, "VALUES (0, 1, 1), (1, 1, 0), (2, 0, 1), (3, 0, 0)");
        }
    }

    @Test
    public void testFiltersGlobalAggregationAndNullValues()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_global_aggregation_null_values",
                "(c1 int, c2 int, c3 int)",
                List.of(
                        "null, null, 0",
                        "null, 1, 0",
                        "1, null, 1",
                        "1, 1, 1"))) {
            String query = String.format("""
                     SELECT count_if(c1 IS NULL), count_if(c2 IS NULL) FROM %s WHERE c3 = 0
                     UNION ALL
                     SELECT count_if(c1 IS NULL), count_if(c2 IS NULL) FROM %s WHERE c3 = 1
                     UNION ALL
                     SELECT count_if(c1 IS NULL), count_if(c2 IS NULL) FROM %s WHERE c3 = 42
                    """, table.getName(), table.getName(), table.getName());

            assertCteReuseApplied(query, 4, 2, 1);

            assertQueryWithAndWithoutCteReuse(query, "VALUES (2, 1), (0, 1), (0, 0)");
        }
    }

    @Test
    public void testFiltersOnNonGroupingColumnsWithEmptyGroups()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_filters_empty_groups",
                "(groupkey int, c1 int, c2 int)",
                List.of(
                        "1, 1, 10",
                        "1, 2, 20",
                        "2, 10, 1",
                        "2, 20, 2",
                        "3, 1, 1",
                        "3, 2, 2",
                        "4, 10, 10",
                        "4, 20, 20"))) {
            String query = String.format("""
                     SELECT 'first_branch', groupkey, count(*) FROM %s WHERE c1 < 5 GROUP BY groupkey
                     UNION ALL
                     SELECT 'second_branch', groupkey, count(*) FROM %s WHERE c2 < 5 GROUP BY groupkey
                    """, table.getName(), table.getName());

            assertCteReuseApplied(query, 5, 4, 2);

            assertQueryWithAndWithoutCteReuse(query, """
                    VALUES
                        ('first_branch', 1, 2),
                        ('first_branch', 3, 2),
                        ('second_branch', 2, 2),
                        ('second_branch', 3, 2)
                    """);
        }
    }

    @Test
    public void testFiltersOnNonGroupingColumnsWithEmptyGroupsOnThreeBranches()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_filters_empty_groups_three_branches",
                "(groupkey int, c1 int, c2 int, c3 int)",
                List.of(
                        "1, 1, 0, 0",
                        "2, 0, 1, 0",
                        "3, 0, 0, 1",
                        "4, 1, 1, 1",
                        "5, 0, 0, 0"))) {
            String query = String.format("""
                     SELECT 'first_branch', groupkey, count(*) FROM %s WHERE c1 = 1 GROUP BY groupkey
                     UNION ALL
                     SELECT 'second_branch', groupkey, count(*) FROM %s WHERE c2 = 1 GROUP BY groupkey
                     UNION ALL
                     SELECT 'third_branch', groupkey, count(*) FROM %s WHERE c3 = 1 GROUP BY groupkey
                    """, table.getName(), table.getName(), table.getName());

            assertCteReuseApplied(query, 7, 5, 2);
            assertQueryWithAndWithoutCteReuse(query, """
                    VALUES
                        ('first_branch', 1, 1),
                        ('first_branch', 4, 1),
                        ('second_branch', 2, 1),
                        ('second_branch', 4, 1),
                        ('third_branch', 3, 1),
                        ('third_branch', 4, 1)
                    """);
        }
    }

    @Test
    public void testFiltersOnNonGroupingColumnsWithEmptyGroupsAndTwoGroupingKeys()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_filters_two_grouping_keys",
                "(g1 int, g2 int, c1 int, c2 int)",
                List.of(
                        "1, 1, 1, 0",
                        "1, 2, 0, 1",
                        "2, 1, 1, 1",
                        "2, 2, 0, 0"))) {
            String query = String.format("""
                     SELECT 'first_branch', g1, g2, count(*) FROM %s WHERE c1 = 1 GROUP BY g1, g2
                     UNION ALL
                     SELECT 'second_branch', g1, g2, count(*) FROM %s WHERE c2 = 1 GROUP BY g1, g2
                    """, table.getName(), table.getName());

            assertCteReuseApplied(query, 5, 4, 2);

            assertQueryWithAndWithoutCteReuse(query, """
                    VALUES
                        ('first_branch', 1, 1, 1),
                        ('first_branch', 2, 1, 1),
                        ('second_branch', 1, 2, 1),
                        ('second_branch', 2, 1, 1)
                    """);
        }
    }

    @Test
    public void testFiltersOnNonGroupingColumnsWithPartialGroupOverlap()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_partial_group_overlap",
                "(groupkey int, value int)",
                List.of(
                        "1, 1",
                        "1, 2",
                        "2, 2",
                        "2, 3",
                        "3, 1",
                        "3, 3",
                        "4, 0",
                        "4, 4"))) {
            String query = String.format("""
                     SELECT 'first', groupkey, count(*) FROM %s WHERE value = 1 GROUP BY groupkey
                     UNION ALL
                     SELECT 'second', groupkey, count(*) FROM %s WHERE value = 2 GROUP BY groupkey
                     UNION ALL
                     SELECT 'third', groupkey, count(*) FROM %s WHERE value = 3 GROUP BY groupkey
                     UNION ALL
                     SELECT 'fourth', groupkey, count(*) FROM %s GROUP BY groupkey
                    """, table.getName(), table.getName(), table.getName(), table.getName());

            assertCteReuseApplied(query, 9, 6, 2);

            assertQueryWithAndWithoutCteReuse(query, """
                    VALUES
                        ('first', 1, 1),
                        ('first', 3, 1),
                        ('second', 1, 1),
                        ('second', 2, 1),
                        ('third', 2, 1),
                        ('third', 3, 1),
                        ('fourth', 1, 2),
                        ('fourth', 2, 2),
                        ('fourth', 3, 2),
                        ('fourth', 4, 2)
                    """);
        }
    }

    @Test
    public void testUnionDistinctWithFiltersOnNonGroupingColumns()
    {
        String query = """
                        SELECT count(*), regionkey FROM nation WHERE nationkey < 3 GROUP BY regionkey
                        UNION DISTINCT
                        SELECT count(*), regionkey FROM nation WHERE nationkey < 5 GROUP BY regionkey
                       """;

        assertCteReuseApplied(query, 3, 2, 1);

        assertQueryWithAndWithoutCteReuse(query, """
                VALUES
                    (2, 1),
                    (1, 0),
                    (3, 1),
                    (1, 4)
                """);
    }

    @Test
    public void testUnionDistinctGlobalAggregations()
    {
        String query = """
                        SELECT count(*) FROM nation WHERE nationkey > 21
                        UNION DISTINCT
                        SELECT count(*) FROM nation WHERE nationkey < 3
                       """;

        assertCteReuseApplied(query, 5, 4, 2);

        assertQueryWithAndWithoutCteReuse(query, "VALUES (3)");
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
