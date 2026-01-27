/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.providers;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.trino.client.NodeVersion;
import io.trino.execution.QueryInfo;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.sql.planner.planprinter.Anonymizer;
import io.trino.sql.planner.planprinter.NoOpAnonymizer;
import io.trino.sql.planner.planprinter.ValuePrinter;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;
import static io.trino.sql.planner.planprinter.PlanPrinter.textDistributedPlan;
import static java.util.Objects.requireNonNull;

public class QueryPlanProvider
        implements TroubleshootingProvider
{
    private final SessionPropertyManager sessionPropertyManager;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final NodeVersion nodeVersion;
    private final Anonymizer anonymizer = new NoOpAnonymizer();

    @Inject
    public QueryPlanProvider(
            SessionPropertyManager sessionPropertyManager,
            Metadata metadata,
            FunctionManager functionManager,
            NodeVersion nodeVersion)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        if (context.has(QueryInfo.class)) {
            Optional<String> plan = createTextQueryPlan(context.getOrThrow(QueryInfo.class));
            if (plan.isPresent()) {
                return ImmutableMap.of("query_plan.txt", toInputStream(plan.get()));
            }
        }

        return ImmutableMap.of();
    }

    private Optional<String> createTextQueryPlan(QueryInfo queryInfo)
    {
        if (queryInfo.getStages().isPresent()) {
            return Optional.of(textDistributedPlan(
                    queryInfo.getStages().get(),
                    queryInfo.getQueryStats(),
                    new ValuePrinter(metadata, functionManager, queryInfo.getSession().toSession(sessionPropertyManager)),
                    false,
                    anonymizer,
                    nodeVersion));
        }
        return Optional.empty();
    }
}
