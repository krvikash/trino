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

import com.google.common.collect.ImmutableMap;
import io.starburst.server.castleinsights.model.TableRef;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().orElseThrow(), "tpch", ImmutableMap.of());
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

        JoinNode join = analysis.planNodes().stream()
                .filter(JoinNode.class::isInstance)
                .map(JoinNode.class::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no join node in plan"));

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

    private CastleQueryAnalysis analyze(@Language("SQL") String sql)
    {
        QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, sql).queryId();
        return new CastleQueryAnalysis(finalQueryInfo(queryId));
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
        return analysis.planNodes().stream()
                .filter(TableScanNode.class::isInstance)
                .map(TableScanNode.class::cast)
                .collect(toImmutableList());
    }

    private static List<TableRef> scanTables(CastleQueryAnalysis analysis)
    {
        return scans(analysis).stream()
                .map(scan -> analysis.table(scan.getId()))
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    private QueryInfo finalQueryInfo(QueryId queryId)
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<QueryInfo> queryInfo = new AtomicReference<>();
        queryRunner.getCoordinator().addFinalQueryInfoListener(queryId, info -> {
            queryInfo.set(info);
            latch.countDown();
        });
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for final query info for query " + queryId);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return queryInfo.get();
    }
}
