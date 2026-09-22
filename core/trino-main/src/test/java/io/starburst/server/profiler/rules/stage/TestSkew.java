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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.starburst.server.profiler.results.RuleFinding;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.UnaryOperator;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.ofBytes;
import static io.starburst.server.profiler.rules.ProfilerFixtures.emptyQueryStats;
import static io.starburst.server.profiler.rules.ProfilerFixtures.jdbcScan;
import static io.starburst.server.profiler.rules.ProfilerFixtures.queryInfo;
import static io.starburst.server.profiler.rules.ProfilerFixtures.taskWithProcessedInput;
import static io.starburst.server.profiler.rules.RuleTestSupport.findings;
import static io.starburst.server.profiler.rules.RuleTestSupport.onlyStageFinding;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSkew
{
    @Test
    public void testFlagsStageWithSkewedTaskInput()
    {
        List<RuleFinding> findings = findings(
                new Skew(),
                queryInfo(
                        jdbcScan("0"),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        emptyQueryStats(),
                        ImmutableList.of(
                                taskWithProcessedInput(ofBytes(1000)),
                                taskWithProcessedInput(ofBytes(1000)),
                                taskWithProcessedInput(ofBytes(1000)),
                                taskWithProcessedInput(ofBytes(1000)),
                                taskWithProcessedInput(DataSize.of(10, MEGABYTE)))),
                UnaryOperator.identity());
        assertThat(onlyStageFinding(findings).finding().description()).contains("skewed");
    }
}
