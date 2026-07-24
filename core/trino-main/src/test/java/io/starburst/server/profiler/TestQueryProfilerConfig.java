/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestQueryProfilerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(QueryProfilerConfig.class)
                .setTopOperatorsPercentage(0.8)
                .setMaxTopOperators(10)
                .setTopStagesPercentage(0.8)
                .setMaxTopStages(10));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.profiler.top-operators-percentage", "0.9")
                .put("query.profiler.max-top-operators", "5")
                .put("query.profiler.top-stages-percentage", "0.7")
                .put("query.profiler.max-top-stages", "8")
                .buildOrThrow();

        QueryProfilerConfig expected = new QueryProfilerConfig()
                .setTopOperatorsPercentage(0.9)
                .setMaxTopOperators(5)
                .setTopStagesPercentage(0.7)
                .setMaxTopStages(8);

        assertFullMapping(properties, expected);
    }
}
