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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BufferNodeStats
{
    private final long totalMemory;
    private final long freeMemory;
    private final int trackedExchanges;
    private final int openChunks;
    private final int closedChunks;

    @JsonCreator
    public BufferNodeStats(
            @JsonProperty("totalMemory") long totalMemory,
            @JsonProperty("freeMemory") long freeMemory,
            @JsonProperty("trackedExchanges") int trackedExchanges,
            @JsonProperty("openChunks") int openChunks,
            @JsonProperty("closedChunks") int closedChunks)
    {
        this.totalMemory = totalMemory;
        this.freeMemory = freeMemory;
        this.trackedExchanges = trackedExchanges;
        this.openChunks = openChunks;
        this.closedChunks = closedChunks;
    }

    @JsonProperty
    public long getTotalMemory()
    {
        return totalMemory;
    }

    @JsonProperty
    public long getFreeMemory()
    {
        return freeMemory;
    }

    @JsonProperty
    public int getTrackedExchanges()
    {
        return trackedExchanges;
    }

    @JsonProperty
    public int getOpenChunks()
    {
        return openChunks;
    }

    @JsonProperty
    public int getClosedChunks()
    {
        return closedChunks;
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
        BufferNodeStats that = (BufferNodeStats) o;
        return totalMemory == that.totalMemory
                && freeMemory == that.freeMemory
                && trackedExchanges == that.trackedExchanges
                && openChunks == that.openChunks
                && closedChunks == that.closedChunks;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(totalMemory, freeMemory, trackedExchanges, openChunks, closedChunks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("totalMemory", totalMemory)
                .add("freeMemory", freeMemory)
                .add("trackedExchanges", trackedExchanges)
                .add("openChunks", openChunks)
                .add("closedChunks", closedChunks)
                .toString();
    }
}
