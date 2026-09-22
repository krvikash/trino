/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules.query;

import io.airlift.units.DataSize;
import io.starburst.server.profiler.results.Finding;
import io.starburst.server.profiler.results.RuleFinding;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyQueryFinding;
import static io.starburst.server.profiler.rules.RuleTestSupport.tpchQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigOutput
{
    @Test
    public void testDetectsBigQueryOutput()
    {
        try (StandaloneQueryRunner queryRunner = tpchQueryRunner()) {
            List<RuleFinding> findings = findings(
                    new BigOutput(),
                    queryRunner,
                    queryRunner.getDefaultSession(),
                    "SELECT * FROM orders",
                    config -> config.setMinBigOutputSize(DataSize.ofBytes(0)));

            Finding finding = onlyQueryFinding(findings).finding();
            assertThat(finding.description()).contains("query output data size is considerable");
        }
    }
}
