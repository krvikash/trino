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
import io.trino.spi.QueryId;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class LocalRemoteCombiningFactory
        implements FlightRecordingFactory
{
    private final FlightRecordingFactory localRecordingFactory;
    private final FlightRecordingFactory remoteRecordingFactory;

    public LocalRemoteCombiningFactory(
            LocalRecordingFactory localRecordingFactory,
            RemoteRecordingFactory remoteRecordingFactory)
    {
        this.localRecordingFactory = requireNonNull(localRecordingFactory, "localRecordingFactory is null");
        this.remoteRecordingFactory = requireNonNull(remoteRecordingFactory, "remoteRecordingFactory is null");
    }

    @Override
    public Optional<FlightRecording> findOrCreate(QueryId queryId, boolean createIfNeeded)
    {
        return localRecordingFactory.findOrCreate(queryId, createIfNeeded)
                .map(localRecording -> new CombinedRecording(queryId, localRecording, remoteRecordingFactory.findByQueryId(queryId)));
    }

    static final class CombinedRecording
            implements FlightRecording
    {
        private final QueryId queryId;
        private final FlightRecording localRecording;
        private final Optional<FlightRecording> remoteRecording;

        public CombinedRecording(QueryId queryId, FlightRecording localRecording, Optional<FlightRecording> remoteRecording)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.localRecording = requireNonNull(localRecording, "localRecording is null");
            this.remoteRecording = requireNonNull(remoteRecording, "remoteRecording is null");
        }

        @Override
        public FlightRecording start()
        {
            this.remoteRecording.ifPresent(FlightRecording::start);
            this.localRecording.start();

            return this;
        }

        @Override
        public void finish()
        {
            this.remoteRecording.ifPresent(FlightRecording::finish);
            this.localRecording.finish();
        }

        @Override
        public void remove()
        {
            this.remoteRecording.ifPresent(FlightRecording::remove);
            this.localRecording.remove();
        }

        @Override
        public void retainForNodes(Set<String> nodeIds)
        {
            this.remoteRecording.ifPresent(recording -> recording.retainForNodes(nodeIds));
        }

        @Override
        public Map<String, InputStream> getInputStreams()
        {
            return ImmutableMap.<String, InputStream>builder()
                    .putAll(localRecording.getInputStreams())
                    .putAll(remoteRecording
                            .map(FlightRecording::getInputStreams)
                            .orElse(ImmutableMap.of()))
                    .buildOrThrow();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("local", localRecording)
                    .add("remote", remoteRecording)
                    .toString();
        }
    }
}
