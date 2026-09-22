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

import static io.starburst.server.profiler.rules.ProfilerFixtures.filter;
import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMissingJdbcFilterPushdown
{
    @Test
    public void testFlagsStaticFilterOverJdbcScan()
    {
        List<RuleFinding> findings = findings(
                new MissingJdbcFilterPushdown(),
                queryInfo(filter("1", jdbcScan("0"))),
                UnaryOperator.identity());
        assertThat(onlyPlanNodeFinding(findings).finding().description()).contains("Missing predicate pushdown");
    }
}
