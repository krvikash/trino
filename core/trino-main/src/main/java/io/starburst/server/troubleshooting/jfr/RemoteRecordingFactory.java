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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

public final class RemoteRecordingFactory
        implements FlightRecordingFactory
{
    private static final Logger log = Logger.get(RemoteRecordingFactory.class);

    private final FlightRecorderHttpClient.WorkerNodesProvider nodesProvider;
    private final FlightRecorderHttpClient.Factory clientFactory;

    @Inject
    public RemoteRecordingFactory(FlightRecorderHttpClient.Factory factory, FlightRecorderHttpClient.WorkerNodesProvider workerNodesProvider)
    {
        this.clientFactory = requireNonNull(factory, "factory is null");
        this.nodesProvider = requireNonNull(workerNodesProvider, "workerNodesProvider is null");
    }

    @Override
    public Optional<FlightRecording> findOrCreate(QueryId queryId, boolean createIfNeeded)
    {
        if (nodesProvider.getWorkerNodes().isEmpty()) {
            return Optional.empty();
        }

        RemoteRecording recording = new RemoteRecording(queryId, clientFactory.create(queryId));
        // Initially keep all nodes as retained
        recording.retainForNodes(nodesProvider.getWorkerNodesIds());
        return Optional.of(recording);
    }

    static final class RemoteRecording
            implements FlightRecording
    {
        private final QueryId queryId;
        private final FlightRecorderHttpClient client;
        private Set<String> currentLiveNodeIds = Set.of();

        public RemoteRecording(QueryId queryId, FlightRecorderHttpClient client)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.client = requireNonNull(client, "client is null");
        }

        @Override
        public FlightRecording start()
        {
            client.start(currentLiveNodeIds);
            log.info("Started %s", this);
            return this;
        }

        @Override
        public void finish()
        {
            client.finish(currentLiveNodeIds);
            log.info("Finished %s", this);
        }

        @Override
        public void remove()
        {
            client.remove(currentLiveNodeIds);
            log.info("Removed %s", this);
        }

        @Override
        public void retainForNodes(Set<String> retainedNodes)
        {
            if (currentLiveNodeIds.isEmpty()) {
                currentLiveNodeIds = Set.copyOf(retainedNodes);
                return;
            }

            Set<String> oldRetainedNodes = Set.copyOf(currentLiveNodeIds);
            currentLiveNodeIds = intersection(oldRetainedNodes, retainedNodes);
            Set<String> removedNodes = difference(oldRetainedNodes, currentLiveNodeIds);
            if (!removedNodes.isEmpty()) {
                client.remove(removedNodes);
                log.info("Retained recordings on: %s, removed on: %s".formatted(currentLiveNodeIds, removedNodes));
            }
        }

        @Override
        public Map<String, InputStream> getInputStreams()
        {
            return client.getInputStreams(currentLiveNodeIds);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("nodes", currentLiveNodeIds)
                    .toString();
        }
    }
}
