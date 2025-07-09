/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record BufferNodeInfo(
        long nodeId,
        URI uri,
        Optional<BufferNodeStats> stats,
        BufferNodeState state,
        Instant timestamp)
{
    public BufferNodeInfo
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(state, "state is null");
    }
}
