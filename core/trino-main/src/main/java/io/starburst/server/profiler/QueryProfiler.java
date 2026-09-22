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
import io.starburst.server.profiler.results.Finding;
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
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.profiler.ConnectorQueryProfilerRegistry;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.profiler.ConnectorQueryProfilerFinding;
import io.trino.spi.profiler.ConnectorQueryProfilerRule;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class QueryProfiler
{
    private static final Logger log = Logger.get(QueryProfiler.class);
    private static final String PROFILER_VERSION = "3.0";

    private final List<QueryProfilerRule> rules;
    private final QueryProfilerConfig config;
    private final NodeVersion nodeVersion;
    private final ConnectorQueryProfilerRegistry connectorQueryProfilerRegistry;

    @Inject
    public QueryProfiler(
            Set<QueryProfilerRule> rules,
            QueryProfilerConfig config,
            NodeVersion nodeVersion,
            ConnectorQueryProfilerRegistry connectorQueryProfilerRegistry)
    {
        this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        this.config = requireNonNull(config, "config is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.connectorQueryProfilerRegistry = requireNonNull(connectorQueryProfilerRegistry, "connectorQueryProfilerRegistry is null");
    }

    public QueryProfilerResult analyze(QueryInfo queryInfo)
    {
        QueryExecutionDetails details = new QueryExecutionDetails(queryInfo, config);
        QueryId queryId = queryInfo.getQueryId();

        ImmutableList.Builder<PlanNodeFinding> planNodeFindings = ImmutableList.builder();
        ImmutableList.Builder<StageFinding> stageFindings = ImmutableList.builder();
        ImmutableList.Builder<QueryFinding> queryFindings = ImmutableList.builder();
        for (QueryProfilerRule rule : rules) {
            String name = rule.getClass().getSimpleName();
            try {
                for (RuleFinding finding : rule.analyze(details)) {
                    switch (finding) {
                        case RuleFinding.Node node -> planNodeFindings.add(new PlanNodeFinding(name, node.planNodeId(), node.finding()));
                        case RuleFinding.Stage stage -> stageFindings.add(new StageFinding(name, stage.stageId(), stage.finding()));
                        case RuleFinding.Query query -> queryFindings.add(new QueryFinding(name, query.finding()));
                    }
                }
            }
            catch (Exception e) {
                log.warn(e, "Profiling rule %s failed for query %s", name, queryId);
            }
        }
        planNodeFindings.addAll(analyzeConnectorInputs(details));

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

    private List<PlanNodeFinding> analyzeConnectorInputs(QueryExecutionDetails details)
    {
        return details.inputs().stream()
                .filter(input -> input.connectorInfo().isPresent() && input.connectorName().isPresent())
                .flatMap(input -> connectorQueryProfilerRegistry.getRules(input.connectorName().get()).stream()
                        .flatMap(rule -> analyze(rule, input).stream()
                                .map(finding -> new PlanNodeFinding(
                                        rule.getClass().getSimpleName(),
                                        input.planNodeId().toString(),
                                        new Finding(finding.description(), finding.severity(), finding.recommendation())))))
                .collect(toImmutableList());
    }

    private List<ConnectorQueryProfilerFinding> analyze(ConnectorQueryProfilerRule rule, Input input)
    {
        try {
            return rule.analyze(input.connectorInfo().orElseThrow());
        }
        catch (Exception e) {
            log.warn(e, "Connector query profiler rule %s failed for catalog %s", rule.getClass().getSimpleName(), input.catalogName());
            return ImmutableList.of();
        }
    }
}
