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
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSameTableScan
{
    @Test
    public void testDetectsExpensiveOverlappingScansOfSameTable()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            List<RuleFinding> findings = findings(
                    new SameTableScan(),
                    queryRunner,
                    queryRunner.getDefaultSession(),
                    "SELECT * FROM nation n1 JOIN nation n2 ON n1.nationkey = n2.regionkey",
                    config -> config.setMinOverlappingScanPercentage(0).setMinOverlappingColumns(0));

            assertThat(findings).hasSize(2)
                    .allSatisfy(finding -> assertThat(finding).isInstanceOf(RuleFinding.Node.class));
            assertThat(findings)
                    .extracting(finding -> ((RuleFinding.Node) finding).description())
                    .allMatch(description -> description.contains("occurs multiple times"));
        }
    }
}
