/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeParallelSplitSourceFactory.PreparedSnowflakeQuery;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.DurationTiming;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.metrics.Metrics;
import jakarta.annotation.Nullable;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFStatement;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.throwIfInvalidWarehouse;
import static com.starburstdata.trino.plugin.snowflake.parallel.ChunkParser.parseChunks;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSplitSource.isEligibleForDynamicFilter;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Split source that executes a Snowflake query and returns splits based on the chunks of results returned.
 */
public class SnowflakeParallelSplitSource
        implements ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(SnowflakeParallelSplitSource.class);

    private final SnowflakeParallelSplitSourceFactory factory;
    private final ConnectorSession session;
    private final Connection connection;
    private final SFSession sfSession;
    private final SFStatement sfStatement;
    private final JdbcTableHandle table;
    private final Executor executor;

    // queryCompletion and delegateSplitSource are volatile, as runQuery and close may run on threads other than getNextBatch.
    // No lock is needed: a close racing runQuery is handled by SFStatement, whose cancel() makes a later executeHelper
    // fail and aborts an already submitted query.

    /**
     * Completes with an empty batch once the Snowflake query completes, or fails with the query failure.
     *
     * <p>Null until the first call to {@link #getNextBatch}.
     */
    @Nullable
    private volatile CompletableFuture<List<ConnectorSplit>> queryCompletion;

    /**
     * A split source with pre-computed splits from the Snowflake response.
     *
     * <p>Null until the Snowflake query is run and completes, then non-null and effectively final.
     */
    @Nullable
    private volatile FixedSplitSource delegateSplitSource;

    // Written by the split-generation thread, read by the engine's split-batch completion callback
    private volatile long queryExecutionNanos = -1;

    SnowflakeParallelSplitSource(
            SnowflakeParallelSplitSourceFactory factory,
            ConnectorSession session,
            Connection connection,
            SFSession sfSession,
            JdbcTableHandle table,
            Executor executor)
    {
        this.factory = factory;
        this.session = session;
        this.connection = connection;
        this.sfSession = sfSession;
        this.sfStatement = new SFStatement(sfSession);
        this.table = table;
        this.executor = requireNonNull(executor, "executor is null");
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
        FixedSplitSource splitSource = delegateSplitSource;
        if (splitSource != null) {
            return splitSource.getNextBatch(maxSize, dynamicFilterSnapshot);
        }
        CompletableFuture<List<ConnectorSplit>> completion = queryCompletion;
        if (completion == null) {
            // Snowflake computes the whole result before returning the first chunk, which can take hours for a large scan.
            // The query must not run on the calling engine thread: the fault-tolerant scheduler calls this method on its
            // only thread, and blocking it there prevents the engine from closing this split source, which is what cancels
            // the Snowflake query, and from finishing the Trino query.
            CompletableFuture<List<ConnectorSplit>> newCompletion = new CompletableFuture<>();
            queryCompletion = newCompletion;
            executor.execute(() -> runQuery(dynamicFilterSnapshot, newCompletion));
            completion = newCompletion;
        }
        // the engine asks for the next batch once the query completes, and gets the splits then
        return completion;
    }

    private void runQuery(DynamicFilterSnapshot dynamicFilterSnapshot, CompletableFuture<List<ConnectorSplit>> completion)
    {
        if (completion.isDone()) {
            // closed before the query started
            return;
        }
        try {
            delegateSplitSource = executeQuery(dynamicFilterSnapshot);
            completion.complete(ImmutableList.of());
        }
        catch (Throwable t) {
            completion.completeExceptionally(t);
        }
    }

    private FixedSplitSource executeQuery(DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        // The query is built here (rather than at split-source creation) so the dynamic filter predicate
        // captured in dynamicFilterSnapshot, after the engine's dynamic-filter wait, is pushed into it.
        PreparedSnowflakeQuery preparedQuery = factory.prepare(session, connection, table, dynamicFilterSnapshot);
        JsonNode jsonResult;
        long executionStart = System.nanoTime();
        try {
            jsonResult = (JsonNode) sfStatement.executeHelper(
                    preparedQuery.query(),
                    "application/snowflake",
                    preparedQuery.bindValues(),
                    false,
                    false,
                    false,
                    new ExecTimeTelemetryData());
        }
        catch (SFException | SnowflakeSQLException e) {
            // TODO: https://starburstdata.atlassian.net/browse/SEP-6500
            throwIfInvalidWarehouse(e);
            throw new TrinoException(JDBC_ERROR, "Couldn't get snowflake splits, %s".formatted(e.getMessage()), e);
        }
        queryExecutionNanos = System.nanoTime() - executionStart;
        logFiltered(jsonResult);
        return new FixedSplitSource(parseChunks(session, jsonResult, sfSession));
    }

    @Override
    public void close()
    {
        FixedSplitSource splitSource = delegateSplitSource;
        CompletableFuture<List<ConnectorSplit>> completion = queryCompletion;
        ImmutableList.Builder<Throwable> closeExceptions = ImmutableList.builder();
        if (splitSource != null) {
            splitSource.close();
        }
        else {
            // delegateSplitSource is null iff the snowflake query is incomplete or not started
            if (completion != null) {
                // keeps a query which has not started yet from running
                completion.cancel(false);
            }
            try {
                sfStatement.cancel();
            }
            catch (SFException | SQLException e) {
                closeExceptions.add(e);
            }
        }
        try (connection) {
            sfStatement.close();
        }
        catch (SQLException e) {
            closeExceptions.add(e);
        }
        ImmutableList<Throwable> thrown = closeExceptions.build();
        if (!thrown.isEmpty()) {
            TrinoException toThrow = new TrinoException(JDBC_ERROR, "Couldn't close split source");
            thrown.forEach(toThrow::addSuppressed);
            throw toThrow;
        }
    }

    @Override
    public boolean isFinished()
    {
        FixedSplitSource splitSource = delegateSplitSource;
        return splitSource != null && splitSource.isFinished();
    }

    /**
     * Exposes the time spent on the Snowflake side while generating splits, so that a stall in
     * Snowflake (e.g. warehouse queueing) is attributable from query stats without JFR profiling.
     * Reported as "splits generation metrics" in the verbose plan and merged into the table scan
     * operator's connector metrics in the query stats API.
     */
    @Override
    public Metrics getMetrics()
    {
        long executionNanos = queryExecutionNanos;
        if (executionNanos < 0) {
            return Metrics.EMPTY;
        }
        return new Metrics(ImmutableMap.of("snowflakeQueryExecutionTime", new DurationTiming(new Duration(executionNanos, NANOSECONDS))));
    }

    /**
     * Debug log non-sensitive information from the JSON object returned by Snowflake.
     */
    private static void logFiltered(JsonNode fullJson)
    {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        ObjectNode filteredJson = JsonNodeFactory.instance.objectNode();

        ImmutableList.of(
                        "code",
                        "message",
                        "success")
                .forEach(column -> filteredJson.set(column, fullJson.path(column)));

        JsonNode dataPath = fullJson.path("data");
        ImmutableList.of(
                        "parameters",
                        "rowtype",
                        "total",
                        "returned",
                        "queryId",
                        "databaseProvider",
                        "finalDatabaseName",
                        "finalSchemaName",
                        "finalWarehouseName",
                        "finalRoleName",
                        "numberOfBinds",
                        "arrayBindSupported",
                        "statementTypeId",
                        "version",
                        "sendResultTime",
                        "queryResultFormat")
                .forEach(column -> filteredJson.set(column, dataPath.path(column)));

        LOG.debug(filteredJson.toPrettyString());
    }
}
