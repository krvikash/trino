/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.common.collect.ImmutableMap;
import io.trino.client.Column;
import io.trino.client.SerializationShim;
import io.trino.client.spooling.Segment;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.client.SerializationShim.fromColumns;
import static io.trino.client.SerializationShim.toColumns;
import static java.util.Objects.requireNonNull;

public record StargateParallelSplit(
        String encoding,
        String columns,
        List<String> segments,
        Map<String, Object> metadata)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(StargateParallelSplit.class);

    public StargateParallelSplit
    {
        requireNonNull(encoding, "encoding is null");
        requireNonNull(columns, "columns is null");
        metadata = ImmutableMap.copyOf(requireNonNull(metadata, "metadata is null"));
        requireNonNull(segments, "segments is null");
    }

    public static StargateParallelSplit create(String encoding, List<Column> columns, List<Segment> segments, Map<String, Object> metadata)
    {
        return new StargateParallelSplit(
                encoding,
                fromColumns(columns),
                segments.stream()
                        .map(SerializationShim::fromSegment)
                        .collect(toImmutableList()),
                metadata);
    }

    public List<Column> getColumns()
    {
        return toColumns(columns);
    }

    public String getSerializedColumns()
    {
        return columns;
    }

    public List<Segment> getSegments()
    {
        return segments
                .stream()
                .map(SerializationShim::toSegment)
                .collect(toImmutableList());
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return Map.of(
                "encoding", encoding,
                "segments", Integer.toString(segments.size()),
                "size", Long.toString(getTotalSegmentsSize()));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getTotalSegmentsSize();
    }

    private long getTotalSegmentsSize()
    {
        return getSegments()
                .stream()
                .mapToLong(Segment::getSegmentSize)
                .sum();
    }
}
