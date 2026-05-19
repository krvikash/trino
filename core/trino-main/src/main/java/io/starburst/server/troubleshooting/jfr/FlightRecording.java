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

import io.starburst.server.troubleshooting.jfr.LocalRecordingFactory.LocalRunningRecording;
import io.starburst.server.troubleshooting.jfr.LocalRecordingFactory.ReadOnlyLocalRecording;
import io.starburst.server.troubleshooting.jfr.LocalRemoteCombiningFactory.CombinedRecording;
import io.starburst.server.troubleshooting.jfr.RemoteRecordingFactory.RemoteRecording;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

public sealed interface FlightRecording
        permits CombinedRecording,
                LocalRunningRecording,
                ReadOnlyLocalRecording,
                RemoteRecording
{
    FlightRecording start();

    void finish();

    void remove();

    default void retainForNodes(Set<String> nodeIds) {}

    Map<String, InputStream> getInputStreams();
}
