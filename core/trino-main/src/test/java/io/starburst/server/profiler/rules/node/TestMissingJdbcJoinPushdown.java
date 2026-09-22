/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules.node;

import io.starburst.server.profiler.results.RuleFinding;
import io.trino.connector.CatalogHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.UnaryOperator;

import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.join;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMissingJdbcJoinPushdown
{
    @Test
    public void testFlagsJoinOfTwoScansFromSameJdbcCatalog()
    {
        List<RuleFinding> findings = findings(
                new MissingJdbcJoinPushdown(),
                queryInfo(join("2", jdbcScan("0"), jdbcScan("1"))),
                UnaryOperator.identity());
        assertThat(onlyPlanNodeFinding(findings).finding().description())
                .isEqualTo("Join is not pushed down to JDBC catalogs [test_catalog].");
    }

    @Test
    public void testDescriptionReportsSingleCatalogForMultiScanSides()
    {
        List<RuleFinding> findings = findings(
                new MissingJdbcJoinPushdown(),
                queryInfo(join("5", join("2", jdbcScan("0"), jdbcScan("1")), jdbcScan("3"))),
                UnaryOperator.identity());
        assertThat(findings)
                .hasSize(2)
                .extracting(finding -> finding.finding().description())
                .allSatisfy(description -> assertThat(description)
                        .isEqualTo("Join is not pushed down to JDBC catalogs [test_catalog]."));
    }

    @Test
    public void testDescriptionReportsAllSharedCatalogsInStableOrder()
    {
        CatalogHandle catalogA = createTestCatalogHandle("catalog_a");
        CatalogHandle catalogB = createTestCatalogHandle("catalog_b");
        List<RuleFinding> findings = findings(
                new MissingJdbcJoinPushdown(),
                queryInfo(join(
                        "6",
                        join("4", jdbcScan("0", catalogA), jdbcScan("1", catalogB)),
                        join("5", jdbcScan("2", catalogA), jdbcScan("3", catalogB)))),
                UnaryOperator.identity());
        assertThat(onlyPlanNodeFinding(findings).finding().description())
                .isEqualTo("Join is not pushed down to JDBC catalogs [catalog_a, catalog_b].");
    }
}
