/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.server.castleinsights.CastleQueryAnalysis;
import io.starburst.server.castleinsights.model.CastleInsight;
import io.starburst.server.castleinsights.model.CastleInsightsResult;
import io.trino.execution.QueryInfo;
import io.trino.spi.NodeVersion;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Collects the insights every {@link CastleInsightAnalyzer} finds in a completed query.
 */
public class CastleInsightsExtractor
{
    private static final Logger log = Logger.get(CastleInsightsExtractor.class);

    private final Set<CastleInsightAnalyzer> analyzers;
    private final NodeVersion nodeVersion;
    private final CastleInsightsStats stats;

    @Inject
    public CastleInsightsExtractor(Set<CastleInsightAnalyzer> analyzers, NodeVersion nodeVersion, CastleInsightsStats stats)
    {
        this.analyzers = ImmutableSet.copyOf(requireNonNull(analyzers, "analyzers is null"));
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public CastleInsightsResult process(QueryInfo queryInfo)
    {
        CastleQueryAnalysis analysis = new CastleQueryAnalysis(queryInfo);

        ImmutableList.Builder<CastleInsight> insights = ImmutableList.builder();
        for (CastleInsightAnalyzer analyzer : analyzers) {
            try {
                insights.addAll(analyzer.analyze(analysis));
                stats.analyzerSucceeded();
            }
            catch (Throwable e) {
                stats.analyzerFailed();
                log.warn(e, "Castle insight analyzer %s failed for query %s", analyzer.getClass().getSimpleName(), analysis.queryId());
            }
        }

        List<CastleInsight> extracted = insights.build();
        stats.queryAnalyzed();
        stats.insightsEmitted(extracted.size());

        return new CastleInsightsResult(
                analysis.queryId(),
                nodeVersion.version(),
                extracted);
    }
}
