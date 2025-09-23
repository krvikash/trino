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
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;

import java.io.InputStream;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FlightRecordingProvider
        implements TroubleshootingProvider
{
    private final FlightRecordingFactory recordingFactory;

    @Inject
    public FlightRecordingProvider(FlightRecordingFactory recordingFactory)
    {
        this.recordingFactory = requireNonNull(recordingFactory, "recordingFactory is null");
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
