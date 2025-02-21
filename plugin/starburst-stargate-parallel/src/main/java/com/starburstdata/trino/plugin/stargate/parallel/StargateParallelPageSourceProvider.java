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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.client.Column;
import io.trino.client.ResultRows;
import io.trino.client.ResultRowsDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.SegmentLoader;
import io.trino.jdbc.StargateInMemoryResultSet;
import io.trino.plugin.base.MappedPageSource;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.client.spooling.DataAttribute.UNCOMPRESSED_SIZE;
import static java.util.Objects.requireNonNull;

public class StargateParallelPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final JdbcClient jdbcClient;
    private final SegmentLoader segmentLoader;

    @Inject
    public StargateParallelPageSourceProvider(JdbcClient jdbcClient, SegmentLoader segmentLoader)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.segmentLoader = requireNonNull(segmentLoader, "segmentLoader is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;

        StargateParallelSplit stargateParallelSplit = (StargateParallelSplit) split;

        ResultRows rows = new ResultRowsDecoder(segmentLoader)
                .toRows(stargateParallelSplit.getColumns(), EncodedQueryData
                        .builder(stargateParallelSplit.encoding())
                        .withAttributes(attributes(stargateParallelSplit.metadata()))
                        .withSegments(stargateParallelSplit.getSegments())
                        .build());

        ImmutableList.Builder<JdbcColumnHandle> handles = ImmutableList.builderWithExpectedSize(columns.size());
        for (ColumnHandle handle : columns) {
            handles.add((JdbcColumnHandle) handle);
        }

        List<JdbcColumnHandle> columnHandles = handles.build();
        List<Integer> fieldIndex = remapColumns(columnHandles, stargateParallelSplit.getColumns());

        return new MappedPageSource(
                new StargateParallelPageSource(
                        jdbcClient,
                        new StargateInMemoryResultSet(stargateParallelSplit.getSerializedColumns(), rows.iterator()),
                        session,
                        tableHandle.getColumns().orElse(columnHandles),
                        getEstimatedDataSize(stargateParallelSplit.getSegments())),
                fieldIndex);
    }

    private DataAttributes attributes(Map<String, Object> metadata)
    {
        DataAttributes.Builder builder = DataAttributes.builder();
        metadata.forEach(builder::set);
        return builder.build();
    }

    // Push aggregations is causing fields to appear in a different order
    private List<Integer> remapColumns(List<JdbcColumnHandle> expectedOrder, List<Column> actualOrder)
    {
        if (expectedOrder.size() != actualOrder.size()) {
            return IntStream.range(0, expectedOrder.size()).boxed().collect(toImmutableList());
        }
        ImmutableMap.Builder<String, Integer> actualIndex = ImmutableMap.builder();
        for (int i = 0; i < actualOrder.size(); i++) {
            actualIndex.put(actualOrder.get(i).getName(), i);
        }

        Map<String, Integer> actualPositions = actualIndex.buildOrThrow();
        ImmutableList.Builder<Integer> fieldMappings = ImmutableList.builderWithExpectedSize(expectedOrder.size());
        for (JdbcColumnHandle jdbcColumnHandle : expectedOrder) {
            fieldMappings.add(actualPositions.get(jdbcColumnHandle.getColumnName()));
        }
        return fieldMappings.build();
    }

    private long getEstimatedDataSize(List<Segment> segments)
    {
        return 3L * segments
                .stream()
                .map(Segment::getMetadata)
                .map(this::extractDataSizeFromAttributes)
                .reduce(0L, Long::sum);
    }

    private long extractDataSizeFromAttributes(DataAttributes attributes)
    {
        return attributes.getOptional(UNCOMPRESSED_SIZE, Integer.class)
                .orElse(attributes.get(SEGMENT_SIZE, Integer.class)); // SEGMENT_SIZE is always present
    }
}
