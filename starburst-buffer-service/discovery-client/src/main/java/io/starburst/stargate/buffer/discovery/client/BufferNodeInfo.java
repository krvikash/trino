/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.discovery.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.net.HostAndPort;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BufferNodeInfo
{
    private final long nodeId;
    private final HostAndPort address;
    private final Optional<BufferNodeStats> stats;
    private final BufferNodeState state;

    @JsonCreator
    public BufferNodeInfo(
            @JsonProperty("nodeId") long nodeId,
            @JsonProperty("address") HostAndPort address,
            @JsonProperty("stats") Optional<BufferNodeStats> stats,
            @JsonProperty("state") BufferNodeState state)
    {
        this.nodeId = nodeId;
        this.address = requireNonNull(address, "externalAddress is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.state = requireNonNull(state, "state is null");
    }

    @JsonProperty
    public long getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public HostAndPort getAddress()
    {
        return address;
    }

    @JsonProperty
    public Optional<BufferNodeStats> getStats()
    {
        return stats;
    }

    @JsonProperty
    public BufferNodeState getState()
    {
        return state;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BufferNodeInfo that = (BufferNodeInfo) o;
        return nodeId == that.nodeId
                && address.equals(that.address)
                && stats.equals(that.stats)
                && state == that.state;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeId, address, stats, state);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeId", nodeId)
                .add("address", address)
                .add("stats", stats)
                .add("state", state)
                .toString();
    }
}
