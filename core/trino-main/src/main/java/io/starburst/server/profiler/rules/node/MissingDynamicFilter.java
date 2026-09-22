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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.profiler.Severity.CRITICAL;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public class MissingDynamicFilter
        implements QueryProfilerRule
{
    private static final Duration ZERO_DURATION = new Duration(0, NANOSECONDS);

    @Override
    public List<RuleFinding> analyze(QueryExecutionDetails details)
    {
        Map<DynamicFilterId, DynamicFilterDomainStats> dynamicFilterById = details.queryStats().getDynamicFiltersStats()
                .getDynamicFilterDomainStats().stream()
                .collect(toMap(DynamicFilterDomainStats::getDynamicFilterId, identity()));

        ImmutableList.Builder<RuleFinding> findings = ImmutableList.builder();
        for (PlanNode node : details.planNodesById().values()) {
            if (!isJoinEligibleForDynamicFilter(node, details)) {
                continue;
            }
            List<PlanNode> sources = details.children(node);
            List<DynamicFilterId> dynamicFilters = dynamicFilters(node);
            long probeSidePositions = details.outputPositions(sources.get(0).getId());
            long buildSidePositions = details.outputPositions(sources.get(1).getId());
            DataSize buildSideSize = details.outputDataSize(sources.get(1).getId());
            long joinOutputPositions = details.outputPositions(node.getId());

            long totalInputPositions = probeSidePositions + buildSidePositions;
            double filteringRatio = totalInputPositions == 0
                    ? 0.0
                    : Math.max(0.0, 1.0 - (double) joinOutputPositions / totalInputPositions) * 100;
            if (filteringRatio / 100.0 <= details.thresholds().dynamicFilterJoinFilteringPercentage()) {
                continue;
            }

            findings.add(toFinding(node, dynamicFilters, dynamicFilterById, probeSidePositions, buildSidePositions, buildSideSize, filteringRatio));
        }
        return findings.build();
    }

    private static String describeDynamicFilter(DynamicFilterId dynamicFilterId, DynamicFilterDomainStats stats)
    {
        Duration collectionDuration = stats == null ? ZERO_DURATION : stats.getCollectionDuration().orElse(ZERO_DURATION);
        String domain = stats == null ? "unknown" : stats.getSimplifiedDomain();
        String domainDescription = "NONE".equals(domain) ? "NONE (build side is empty)" : domain;
        return "    - %s: domain=%s, collection time=%s".formatted(dynamicFilterId, domainDescription, collectionDuration);
    }

    private static boolean isJoinEligibleForDynamicFilter(PlanNode node, QueryExecutionDetails details)
    {
        boolean eligibleJoin = node instanceof JoinNode joinNode
                && (joinNode.getType() == JoinType.INNER || joinNode.getType() == JoinType.RIGHT);
        if (!eligibleJoin && !(node instanceof SemiJoinNode)) {
            return false;
        }
        return details.outputPositions(details.children(node).getFirst().getId()) > 0;
    }

    private static List<DynamicFilterId> dynamicFilters(PlanNode node)
    {
        if (node instanceof JoinNode joinNode) {
            return ImmutableList.copyOf(joinNode.getDynamicFilters().keySet());
        }
        if (node instanceof SemiJoinNode semiJoinNode && semiJoinNode.getDynamicFilterId().isPresent()) {
            return ImmutableList.of(semiJoinNode.getDynamicFilterId().orElseThrow());
        }
        return ImmutableList.of();
    }

    private static RuleFinding toFinding(
            PlanNode node,
            List<DynamicFilterId> dynamicFilters,
            Map<DynamicFilterId, DynamicFilterDomainStats> dynamicFilterById,
            long probeSidePositions,
            long buildSidePositions,
            DataSize buildSideSize,
            double filteringRatio)
    {
        String probeSummary = "probe: %s rows, build side: %s rows (%s), filtering ratio: %3.1f%%".formatted(
                probeSidePositions, buildSidePositions, buildSideSize.succinct(), filteringRatio);
        String explanation;
        if (dynamicFilters.isEmpty()) {
            explanation = "No dynamic filter on join — %s".formatted(probeSummary);
        }
        else {
            String filterDetails = dynamicFilters.stream()
                    .map(dynamicFilterId -> describeDynamicFilter(dynamicFilterId, dynamicFilterById.get(dynamicFilterId)))
                    .collect(joining("\n"));
            explanation = "Dynamic filters not reducing probe scan effectively — %s:\n%s".formatted(probeSummary, filterDetails);
        }
        return new RuleFinding.Node(node.getId().toString(), explanation, CRITICAL, Optional.empty());
    }
}
