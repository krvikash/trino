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

import io.airlift.units.DataSize;
import io.starburst.server.profiler.results.Finding;
import io.starburst.server.profiler.results.RuleFinding;
import io.trino.Session;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.StandaloneQueryRunner;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWrongJoinDistributionType
{
    @Test
    public void testDetectsReplicatedJoinWithBuildSideAboveBroadcastThreshold()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            Session session = Session.builder(TEST_SESSION)
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                    .build();

            List<RuleFinding> findings = findings(
                    new WrongJoinDistributionType(),
                    queryRunner,
                    session,
                    "SELECT * FROM region r JOIN nation n ON r.regionkey = n.regionkey",
                    config -> config.setJoinMaxBroadcastTableSize(DataSize.ofBytes(1)));

            Finding finding = onlyPlanNodeFinding(findings).finding();
            assertThat(finding.recommendation()).get(InstanceOfAssertFactories.STRING)
                    .contains("should be PARTITIONED");
        }
    }
}
