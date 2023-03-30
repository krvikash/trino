/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class PartitionNodeMapping
{
    private final Map<Integer, Long> mapping;

    @JsonCreator
    public PartitionNodeMapping(@JsonProperty("mapping") Map<Integer, Long> mapping)
    {
        this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
    }

    @JsonProperty
    public Map<Integer, Long> mapping()
    {
        return mapping;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (PartitionNodeMapping) obj;
        return Objects.equals(this.mapping, that.mapping);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(mapping);
    }

    @Override
    public String toString()
    {
        return "PartitionNodeMapping[" +
                "mapping=" + mapping + ']';
    }
}
