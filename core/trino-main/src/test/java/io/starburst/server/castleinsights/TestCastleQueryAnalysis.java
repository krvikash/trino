/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.common.collect.ImmutableList;
import io.starburst.server.castleinsights.CastleQueryAnalysis.ColumnRef;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.operator.join.JoinOperatorInfo;
import io.trino.spi.QueryId;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.server.castleinsights.CastleInsightsTestHelper.createTpchQueryRunner;
import static io.starburst.server.castleinsights.CastleInsightsTestHelper.finalQueryInfo;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestCastleQueryAnalysis
{
    @Language("SQL")
    private static final String JOIN_QUERY = "SELECT n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey";

    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        queryRunner = createTpchQueryRunner();
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testResolvesScannedTables()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);

        assertThat(scanTables(analysis))
                .containsExactlyInAnyOrder(
                        new TableRef("tpch", "tiny", "nation"),
                        new TableRef("tpch", "tiny", "region"));
    }

    @Test
    public void testResolvesJoinColumnNamesFromSymbols()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);

        JoinNode join = onlyJoin(analysis);

        assertThat(join.getCriteria()).hasSize(1);
        EquiJoinClause clause = join.getCriteria().getFirst();

        assertThat(columnBehind(analysis, clause.getLeft()))
                .as("left side of the join criteria resolves back to the real column name")
                .contains("regionkey");
        assertThat(columnBehind(analysis, clause.getRight()))
                .as("right side of the join criteria resolves back to the real column name")
                .contains("regionkey");
    }

    @Test
    public void testTraversesAcrossFragmentBoundaries()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);

        assertThat(analysis.rootNode()).isPresent();
        assertThat(scans(analysis))
                .as("both scans are reachable from the root, so remote sources were followed into their fragments")
                .hasSize(2);
    }

    @Test
    public void testReportsRowCountsForScans()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);

        for (TableScanNode scan : scans(analysis)) {
            TableRef table = analysis.table(scan.getId()).orElseThrow();
            long expected = switch (table.table()) {
                case "nation" -> 25L;
                case "region" -> 5L;
                default -> throw new AssertionError("unexpected table " + table);
            };
            assertThat(analysis.outputRowCount(scan.getId()))
                    .as("measured rows for %s", table)
                    .isEqualTo(expected);
            assertThat(analysis.estimatedRowCount(scan.getId()))
                    .as("estimated rows for %s", table)
                    .hasValue(expected);
        }
    }

    @Test
    public void testMetadataOnlyQueryHasNoPlan()
    {
        CastleQueryAnalysis analysis = analyze("SET SESSION join_distribution_type = 'BROADCAST'");

        assertThat(analysis.rootNode()).isEmpty();
        assertThat(analysis.planNodes()).isEmpty();
        assertThat(analysis.table(new PlanNodeId("0"))).isEmpty();
    }

    @Test
    public void testPlanNodesFilteredByType()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);

        assertThat(analysis.planNodes(JoinNode.class)).hasSize(1);
        assertThat(analysis.planNodes(TableScanNode.class)).hasSize(2);
    }

    @Test
    public void testPlanNodesFilteredByPredicate()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);

        assertThat(analysis.planNodes(TableScanNode.class::isInstance))
                .containsExactlyInAnyOrderElementsOf(analysis.planNodes(TableScanNode.class));
    }

    @Test
    public void testTraceColumnResolvesJoinKeysThroughThePlan()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);
        JoinNode join = onlyJoin(analysis);
        EquiJoinClause clause = join.getCriteria().getFirst();

        ColumnRef left = analysis.traceColumn(join.getLeft(), clause.getLeft()).orElseThrow();
        ColumnRef right = analysis.traceColumn(join.getRight(), clause.getRight()).orElseThrow();

        assertThat(left.column()).isEqualTo("regionkey");
        assertThat(right.column()).isEqualTo("regionkey");
        assertThat(left.table()).isNotEqualTo(right.table());
        assertThat(ImmutableList.of(left.table(), right.table()))
                .containsExactlyInAnyOrder(new TableRef("tpch", "tiny", "nation"), new TableRef("tpch", "tiny", "region"));
    }

    @Test
    public void testTraceColumnFollowsACast()
    {
        CastleQueryAnalysis analysis = analyze("SELECT n.name FROM nation n JOIN region r ON CAST(n.regionkey AS bigint) = r.regionkey");
        JoinNode join = onlyJoin(analysis);
        EquiJoinClause clause = join.getCriteria().getFirst();

        assertThat(analysis.traceColumn(join.getLeft(), clause.getLeft()).map(ColumnRef::column))
                .as("the cast on the left key is traced through to the column")
                .contains("regionkey");
    }

    @Test
    public void testTraceColumnEmptyWhenFanInDisagrees()
    {
        // The left side of the join is a UNION of two different columns (nationkey and regionkey), so the
        // joined symbol fans in to columns that disagree and must not be attributed to an arbitrary one.
        CastleQueryAnalysis analysis = analyze(
                "SELECT k FROM (SELECT nationkey AS k FROM nation UNION ALL SELECT regionkey AS k FROM nation) u " +
                        "JOIN region r ON u.k = r.regionkey");
        JoinNode join = onlyJoin(analysis);
        EquiJoinClause clause = join.getCriteria().getFirst();

        assertThat(analysis.traceColumn(join.getLeft(), clause.getLeft()))
                .as("a join key fed by a union of different columns does not trace to a single column")
                .isEmpty();
    }

    @Test
    public void testTraceColumnEmptyForComputedExpression()
    {
        CastleQueryAnalysis analysis = analyze("SELECT n.name FROM nation n JOIN region r ON n.regionkey + 1 = r.regionkey");
        JoinNode join = onlyJoin(analysis);
        EquiJoinClause clause = join.getCriteria().getFirst();

        assertThat(analysis.traceColumn(join.getLeft(), clause.getLeft()))
                .as("a computed join key does not trace to a column")
                .isEmpty();
    }

    @Test
    public void testOperatorStatsReachableForJoinNode()
    {
        CastleQueryAnalysis analysis = analyze(JOIN_QUERY);
        JoinNode join = onlyJoin(analysis);

        assertThat(analysis.operatorStats(join.getId()))
                .as("the join node carries operator stats, at least one being join operator info")
                .anyMatch(operator -> operator.getInfo() instanceof JoinOperatorInfo);
    }

    private CastleQueryAnalysis analyze(@Language("SQL") String sql)
    {
        QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, sql).queryId();
        return new CastleQueryAnalysis(finalQueryInfo(queryRunner, queryId));
    }

    private static JoinNode onlyJoin(CastleQueryAnalysis analysis)
    {
        return getOnlyElement(analysis.planNodes(JoinNode.class));
    }

    private static Optional<String> columnBehind(CastleQueryAnalysis analysis, Symbol symbol)
    {
        return scans(analysis).stream()
                .map(scan -> analysis.columnName(scan.getId(), symbol.name()))
                .flatMap(Optional::stream)
                .findFirst();
    }

    private static List<TableScanNode> scans(CastleQueryAnalysis analysis)
    {
        return analysis.planNodes(TableScanNode.class);
    }

    private static List<TableRef> scanTables(CastleQueryAnalysis analysis)
    {
        return scans(analysis).stream()
                .map(scan -> analysis.table(scan.getId()))
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }
}
