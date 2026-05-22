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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.client.Column;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementClient;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugin.stargate.parallel.LiteralFormatter.formatLiteral;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSplitSource.isEligibleForDynamicFilter;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.getDynamicFilteringWaitTimeout;
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
    private final StargateClientFactory clientFactory;
    private final JdbcClient stargateClient;
    private final RemoteQueryModifier queryModifier;
    private final ConnectorSession session;
    private final JdbcTableHandle table;
    private final AtomicReference<DataAttributes> metadata = new AtomicReference<>();

    // Built lazily on the first getNextBatch so the dynamic filter predicate captured after the
    // engine's dynamic-filter wait can be pushed into the remote query.
    private StatementClient client;

    public StargateParallelSplitSource(
            ExecutorService executor,
            StargateClientFactory clientFactory,
            JdbcClient stargateClient,
            RemoteQueryModifier queryModifier,
            ConnectorSession session,
            JdbcTableHandle table)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.stargateClient = requireNonNull(stargateClient, "stargateClient is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.session = requireNonNull(session, "session is null");
        this.table = requireNonNull(table, "table is null");
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        if (!dynamicFilteringEnabled(session) || !isEligibleForDynamicFilter(table)) {
            return 0;
        }
        return getDynamicFilteringWaitTimeout(session).toMillis();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        return supplyAsync(() -> prepareNextBatch(maxSize, dynamicFilterSnapshot), executor);
    }

    private List<ConnectorSplit> prepareNextBatch(int maxBatchSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        if (client == null) {
            client = createClient(dynamicFilterSnapshot);
        }

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

    private StatementClient createClient(DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        List<JdbcColumnHandle> columns = table.getColumns()
                .orElseGet(() -> stargateClient.getColumns(session, table));

        PreparedQuery preparedQuery = stargateClient.prepareQuery(
                session,
                dynamicFilteringEnabled(session) && isEligibleForDynamicFilter(table) ? table.intersectedWithConstraint(dynamicFilterSnapshot.currentPredicate()) : table,
                Optional.empty(),
                columns,
                Map.of());

        return clientFactory.createFactory(getExecuteStatement(preparedQuery));
    }

    private String getExecuteStatement(PreparedQuery preparedQuery)
    {
        List<QueryParameter> parameters = preparedQuery.parameters();
        String finalQuery = queryModifier.apply(session, preparedQuery.query());

        if (parameters.isEmpty()) {
            return finalQuery;
        }

        List<String> binds = parameters.stream()
                .map(parameter -> bindParameter(parameter.getType(), parameter.getValue().orElseThrow()))
                .collect(toImmutableList());

        return """
               EXECUTE IMMEDIATE '%s' USING %s
               """.formatted(finalQuery.replace("'", "''"), Joiner.on(",").join(binds));
    }

    private String bindParameter(Type type, Object value)
    {
        return stargateClient.toWriteMapping(session, type)
                .getWriteFunction()
                .getBindExpression()
                .replace("?", formatLiteral(type, value));
    }

    private static StargateParallelSplit createSplit(String encoding, List<Column> columns, List<Segment> segments, DataAttributes attributes)
    {
        return StargateParallelSplit.create(encoding, columns, segments, attributes.toMap());
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        return client != null && !client.isRunning();
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
