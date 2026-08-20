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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.UnaryOperator;

import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.join;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
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
        assertThat(onlyPlanNodeFinding(findings).description()).contains("missing join pushdown");
    }
}
