/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules.stage;

import io.starburst.server.profiler.results.RuleFinding;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSlowSplitDistribution
{
    @Test
    public void testDetectsStageSpendingTimeGeneratingSplits()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            List<RuleFinding> findings = findings(
                    new SlowSplitDistribution(),
                    queryRunner,
                    queryRunner.getDefaultSession(),
                    "SELECT * FROM lineitem",
                    config -> config.setSlowSplitDistributionPercentage(0));

            assertThat(findings)
                    .isNotEmpty()
                    .allSatisfy(finding -> assertThat(((RuleFinding.Stage) finding).finding().description())
                            .contains("split generation"));
        }
    }
}
