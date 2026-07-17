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
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeSQLException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.throwIfInvalidWarehouse;
import static com.starburstdata.trino.plugin.snowflake.parallel.ChunkParser.parseChunks;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSplitSource.isEligibleForDynamicFilter;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
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

    /**
     * A split source with pre-computed splits from the Snowflake response.
     *
     * <p>Null until the Snowflake query is run and completes, then non-null and effectively final.
     */
    @Nullable
    private FixedSplitSource delegateSplitSource;

    // Written by the split-generation thread, read by the engine's split-batch completion callback
    private volatile long queryExecutionNanos = -1;

    SnowflakeParallelSplitSource(
            SnowflakeParallelSplitSourceFactory factory,
            ConnectorSession session,
            Connection connection,
            SFSession sfSession,
            JdbcTableHandle table)
    {
        this.factory = factory;
        this.session = session;
        this.connection = connection;
        this.sfSession = sfSession;
        this.sfStatement = new SFStatement(sfSession);
        this.table = table;
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
        return getSplitSource(dynamicFilterSnapshot).getNextBatch(maxSize, dynamicFilterSnapshot);
    }

    private FixedSplitSource getSplitSource(DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        if (delegateSplitSource == null) {
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
            delegateSplitSource = new FixedSplitSource(parseChunks(session, jsonResult, sfSession));
        }
        return delegateSplitSource;
    }

    @Override
    public void close()
    {
        ImmutableList.Builder<Throwable> closeExceptions = ImmutableList.builder();
        if (delegateSplitSource != null) {
            delegateSplitSource.close();
        }
        else {
            // delegateSplitSource is null iff the snowflake query is incomplete or not started
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
        return delegateSplitSource != null && delegateSplitSource.isFinished();
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
