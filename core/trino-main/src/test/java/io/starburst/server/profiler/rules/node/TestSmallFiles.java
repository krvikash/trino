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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.starburst.server.profiler.results.RuleFinding;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.units.DataSize.ofBytes;
import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryStats;
import static io.starburst.server.profiler.rules.ProfilerFixtures.smallFilesOperator;
import static io.starburst.server.profiler.rules.ProfilerFixtures.tables;
import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyPlanNodeFinding;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSmallFiles
{
    @Test
    public void testFlagsScanProducingManySmallSplits()
    {
        List<RuleFinding> findings = findings(
                new SmallFiles(),
                queryInfo(
                        jdbcScan("0"),
                        ImmutableList.of(),
                        tables("0", "events"),
                        queryStats(
                                ofBytes(0),
                                new Duration(0, SECONDS),
                                new Duration(0, SECONDS),
                                new Duration(0, SECONDS),
                                ImmutableList.of(smallFilesOperator("0", 2000, 5.0))),
                        ImmutableList.of()),
                config -> config.setSmallFilesMinNumberOfSplits(1));
        assertThat(onlyPlanNodeFinding(findings).finding().description())
                .contains("test_catalog.test_schema.events")
                .contains("small splits")
                .doesNotContain("Table scan 0");
    }
}
