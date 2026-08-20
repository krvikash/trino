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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;

import java.util.List;

public class BigOutput
        implements QueryProfilerRule
{
    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        if (isBigOutput(details)) {
            return ImmutableList.of(toFinding(details));
        }
        return ImmutableList.of();
    }

    private static boolean isBigOutput(QueryExecutionDetails details)
    {
        DataSize queryOutputDataSize = details.queryStats().getOutputDataSize();
        return queryOutputDataSize.toBytes() > details.thresholds().minBigOutputSize().toBytes();
    }

    private static RuleFinding toFinding(QueryExecutionDetails details)
    {
        return new RuleFinding.Query("The query output data size is considerable %s".formatted(details.queryStats().getOutputDataSize()));
    }
}
