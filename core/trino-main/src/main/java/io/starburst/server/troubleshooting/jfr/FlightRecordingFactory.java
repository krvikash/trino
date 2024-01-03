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

import io.trino.spi.QueryId;

import java.util.Optional;

public sealed interface FlightRecordingFactory
        permits RemoteRecordingFactory, LocalRecordingFactory, LocalRemoteCombiningFactory
{
    Optional<FlightRecording> findOrCreate(QueryId queryId, boolean createIfNeeded);

    default Optional<FlightRecording> findByQueryId(QueryId queryId)
    {
        return findOrCreate(queryId, false);
    }

    default FlightRecording createStarted(QueryId queryId)
    {
        return findOrCreate(queryId, true)
                .orElseThrow()
                .start();
    }

    default void cleanup() {}
}
