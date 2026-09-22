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

import io.starburst.server.profiler.results.Finding;
import io.starburst.server.profiler.results.RuleFinding;
import io.trino.Session;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMissingDynamicFilter
{
    @Test
    public void testDetectsJoinWithoutEffectiveDynamicFilter()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            Session session = Session.builder(TEST_SESSION)
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                    .build();

            List<RuleFinding> findings = findings(
                    new MissingDynamicFilter(),
                    queryRunner,
                    session,
                    "SELECT * FROM nation n JOIN region r ON n.regionkey = r.regionkey",
                    config -> config.setDynamicFilterJoinFilteringPercentage(-1));

            Finding finding = onlyPlanNodeFinding(findings).finding();
            assertThat(finding.description()).contains("No dynamic filter on join");
        }
    }
}
