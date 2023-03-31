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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class PartitionNodeMapping
{
    private final ListMultimap<Integer, Long> mapping;

    public PartitionNodeMapping(ListMultimap<Integer, Long> mapping)
    {
        this.mapping = ImmutableListMultimap.copyOf(requireNonNull(mapping, "mapping is null"));
    }

    @JsonCreator
    // specialized @JsonCreator as we cannot use ListMultimap for JSON serialization in this context
    @Deprecated
    public static PartitionNodeMapping createFromMappingAsMap(@JsonProperty("mapping") Map<Integer, List<Long>> mappingAsMap)
    {
        requireNonNull(mappingAsMap, "mappingAsMap is null");
        ImmutableListMultimap.Builder<Integer, Long> mappingBuilder = ImmutableListMultimap.builder();
        mappingAsMap.forEach(mappingBuilder::putAll);
        return new PartitionNodeMapping(mappingBuilder.build());
    }

    @JsonProperty("mapping")
    @Deprecated // just for JSON serialization
    public Map<Integer, List<Long>> getMappingAsMap()
    {
        ImmutableMap.Builder<Integer, List<Long>> builder = ImmutableMap.builder();
        mapping.asMap().forEach((key, value) -> builder.put(key, (List<Long>) value));
        return builder.buildOrThrow();
    }

    @JsonIgnore
    public ListMultimap<Integer, Long> getMapping()
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
