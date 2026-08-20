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
import io.trino.Session;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWrongJoinOrder
{
    @Test
    public void testDetectsBuildSideBiggerThanProbeSide()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            Session session = Session.builder(TEST_SESSION)
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                    .build();

            List<RuleFinding> findings = findings(
                    new WrongJoinOrder(),
                    queryRunner,
                    session,
                    // Reordering disabled and the big table (orders) written on the build (right) side.
                    "SELECT * FROM region r JOIN orders o ON r.regionkey = o.custkey",
                    config -> config);

            RuleFinding.Node finding = onlyPlanNodeFinding(findings);
            assertThat(finding.description())
                    .contains("Build side")
                    .contains("join_reordering_strategy is set to 'NONE'");
        }
    }
}
