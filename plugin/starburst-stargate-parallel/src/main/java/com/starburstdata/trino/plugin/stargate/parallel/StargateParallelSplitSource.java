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
import io.airlift.units.DataSize;
import io.trino.client.Column;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementClient;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.min;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StargateParallelSplitSource
        implements ConnectorSplitSource
{
    private static final DataSize TARGET_SPLIT_SIZE = DataSize.of(128, MEGABYTE);
    private static final int MIN_SCHEDULABLE_BATCH = 8;
    private static final long MIN_WINDOW_TIMEOUT = SECONDS.toNanos(3);

    private final ExecutorService executor;
    private final StatementClient client;
    private final AtomicReference<DataAttributes> metadata = new AtomicReference<>();

    public StargateParallelSplitSource(ExecutorService executor, StatementClient client)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return supplyAsync(() -> new ConnectorSplitBatch(prepareNextBatch(maxSize), isFinished()), executor);
    }

    private List<ConnectorSplit> prepareNextBatch(int maxBatchSize)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        int currentBatchSize = 0;
        long nanoStartTime = nanoTime();
        while ((currentBatchSize < min(MIN_SCHEDULABLE_BATCH, maxBatchSize) && (nanoTime() - nanoStartTime < MIN_WINDOW_TIMEOUT)) && client.advance()) {
            if (!client.isRunning()) {
                break;
            }

            QueryResults results = client.currentQueryResults();
            if (results.getData() == null) {
                continue; // No data yet
            }

            if (results.getData() instanceof EncodedQueryData encodedData) {
                metadata.compareAndSet(null, encodedData.getMetadata());

                for (List<Segment> segments : partitionSegments(encodedData.getSegments())) {
                    splits.add(createSplit(encodedData.getEncoding(), results.getColumns(), segments, metadata.get()));
                    currentBatchSize++;
                }
            }
            else {
                throw new IllegalStateException("Received inline data when encoded was expected");
            }
        }

        if (client.isClientAborted()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Remote query was aborted");
        }

        QueryError queryError = client.currentStatusInfo().getError();
        if (queryError != null) {
            if (queryError.getFailureInfo() != null) {
                throw queryError.getFailureInfo().toException();
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Remote query failed");
            }
        }
        return splits.build();
    }

    private static StargateParallelSplit createSplit(String encoding, List<Column> columns, List<Segment> segments, DataAttributes attributes)
    {
        return StargateParallelSplit.create(encoding, columns, segments, attributes.toMap());
    }

    @Override
    public void close()
    {
        client.close();
    }

    @Override
    public boolean isFinished()
    {
        return !client.isRunning();
    }

    private static List<List<Segment>> partitionSegments(List<Segment> segments)
    {
        ImmutableList.Builder<List<Segment>> partitions = ImmutableList.builder();
        List<Segment> currentBatch = new ArrayList<>();
        for (Segment segment : segments) {
            currentBatch.add(segment);
            if (sizeOf(currentBatch) >= TARGET_SPLIT_SIZE.toBytes()) {
                partitions.add(currentBatch);
                currentBatch = new ArrayList<>();
            }
        }
        if (!currentBatch.isEmpty()) {
            partitions.add(currentBatch);
        }
        return partitions.build();
    }

    private static long sizeOf(List<Segment> segments)
    {
        return segments.stream()
                .mapToLong(Segment::getSegmentSize)
                .sum();
    }
}
