/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.StageInfo.getAllStages;
import static java.util.Objects.requireNonNull;

public class FlightRecordingProvider
        implements TroubleshootingProvider
{
    private final FlightRecordingFactory recordingFactory;
    private final QueryManager queryManager;

    @Inject
    public FlightRecordingProvider(FlightRecordingFactory recordingFactory, QueryManager queryManager)
    {
        this.recordingFactory = requireNonNull(recordingFactory, "recordingFactory is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @Override
    public void onContextStarted(TroubleshootingContext context)
    {
        context.set(FlightRecording.class, recordingFactory.createStarted(context.getQueryId()));
    }

    @Override
    public void onContextFinished(TroubleshootingContext context)
    {
        FlightRecording recording = context.getOrThrow(FlightRecording.class);
        recording.retainForNodes(context.getJfrCollectedNodes());
        recording.finish();
    }

    private Set<String> getProcessingNodesForQuery(QueryId queryId)
    {
        try {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            return queryInfo.getOutputStage().map(this::getNodeIdsProcessingQuery).orElse(ImmutableSet.of());
        }
        catch (Exception e) {
            return Set.of();
        }
    }

    private Set<String> getNodeIdsProcessingQuery(StageInfo outputStage)
    {
        List<TaskInfo> tasks = getAllStages(Optional.of(outputStage)).stream()
                .map(StageInfo::getTasks)
                .flatMap(Collection::stream)
                .collect(toImmutableList());

        return tasks.stream()
                .map(TaskInfo::getTaskStatus)
                .map(TaskStatus::getNodeId)
                .collect(toImmutableSet());
    }

    @Override
    public void onContextRemoved(TroubleshootingContext context)
    {
        context.getOrThrow(FlightRecording.class).remove();
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return context.get(FlightRecording.class)
                .map(FlightRecording::getInputStreams)
                .orElse(ImmutableMap.of());
    }
}
