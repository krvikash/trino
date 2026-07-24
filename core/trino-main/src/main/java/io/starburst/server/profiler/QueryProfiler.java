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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.server.profiler.results.PlanNodeFinding;
import io.starburst.server.profiler.results.QueryFinding;
import io.starburst.server.profiler.results.QueryProfilerResult;
import io.starburst.server.profiler.results.QueryStatsSummary;
import io.starburst.server.profiler.results.QueryStatsSummaryFactory;
import io.starburst.server.profiler.results.QuerySummary;
import io.starburst.server.profiler.results.QuerySummaryFactory;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.results.StageFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.execution.QueryInfo;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class QueryProfiler
{
    private static final Logger log = Logger.get(QueryProfiler.class);
    private static final String PROFILER_VERSION = "1.0";

    private final List<QueryProfilerRule> rules;
    private final QueryProfilerConfig config;
    private final NodeVersion nodeVersion;

    @Inject
    public QueryProfiler(Set<QueryProfilerRule> rules, QueryProfilerConfig config, NodeVersion nodeVersion)
    {
        this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        this.config = requireNonNull(config, "config is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    public QueryProfilerResult analyze(QueryInfo queryInfo)
    {
        QueryExecutionDetails details = new QueryExecutionDetails(queryInfo, config);
        QueryId queryId = queryInfo.getQueryId();

        ImmutableList.Builder<PlanNodeFinding> planNodeFindings = ImmutableList.builder();
        ImmutableList.Builder<StageFinding> stageFindings = ImmutableList.builder();
        ImmutableList.Builder<QueryFinding> queryFindings = ImmutableList.builder();
        for (QueryProfilerRule rule : rules) {
            try {
                for (RuleFinding finding : rule.analyze(details)) {
                    switch (finding) {
                        case PlanNodeFinding nodeFinding -> planNodeFindings.add(nodeFinding);
                        case StageFinding stageFinding -> stageFindings.add(stageFinding);
                        case QueryFinding queryFinding -> queryFindings.add(queryFinding);
                        default -> throw new IllegalArgumentException("Illegal rule finding type");
                    }
                }
            }
            catch (Exception e) {
                log.warn(e, "Profiling rule %s failed for query %s", rule.getClass().getSimpleName(), queryId);
            }
        }

        QueryStatsSummary statsSummary = QueryStatsSummaryFactory.from(queryInfo);
        QuerySummary summary = QuerySummaryFactory.from(details);
        return new QueryProfilerResult(
                queryId.toString(),
                PROFILER_VERSION,
                nodeVersion.version(),
                planNodeFindings.build(),
                stageFindings.build(),
                queryFindings.build(),
                statsSummary,
                summary);
    }
}
