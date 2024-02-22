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
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    private final int maxCollectedWorkersJfr;
    private final InternalNodeManager internalNodeManager;

    @Inject
    public FlightRecordingProvider(FlightRecordingFactory recordingFactory, QueryManager queryManager, InternalNodeManager internalNodeManager, FlightRecorderConfig config)
    {
        this.recordingFactory = requireNonNull(recordingFactory, "recordingFactory is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.maxCollectedWorkersJfr = config.getMaxCollectedWorkersJfr();
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
        recording.retainForNodes(limitWorkerNodes(internalNodeManager, getProcessingNodesForQuery(context.getQueryId()), maxCollectedWorkersJfr));
        recording.finish();
    }

    private static Set<String> limitWorkerNodes(InternalNodeManager nodeManager, Set<String> nodes, int maxCollectedWorkers)
    {
        if (nodes.size() <= maxCollectedWorkers) {
            return nodes;
        }
        // first split input nodes to workers and coordinators
        Set<String> coordinatorIds = nodeManager.getCoordinators().stream().map(InternalNode::getNodeIdentifier).collect(toImmutableSet());
        List<String> workers = new ArrayList<>(nodes.size());
        List<String> coordinators = new ArrayList<>();
        for (String node : nodes) {
            if (coordinatorIds.contains(node)) {
                coordinators.add(node);
            }
            else {
                workers.add(node);
            }
        }

        if (workers.size() <= maxCollectedWorkers) {
            return nodes;
        }

        // then choose maxCollectedWorkers workers randomly
        Collections.shuffle(workers);
        return ImmutableSet.<String>builder()
                .addAll(coordinators)
                .addAll(workers.subList(0, maxCollectedWorkers))
                .build();
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
