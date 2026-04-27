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
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.FixedSplitSource;
import jakarta.annotation.Nullable;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeSQLException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.throwIfInvalidWarehouse;
import static com.starburstdata.trino.plugin.snowflake.parallel.ChunkParser.parseChunks;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

/**
 * Split source that executes a Snowflake query and returns splits based on the chunks of results returned.
 */
public class SnowflakeParallelSplitSource
        implements ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(SnowflakeParallelSplitSource.class);

    private final ConnectorSession session;
    private final Connection connection;
    private final SFSession sfSession;
    private final SFStatement sfStatement;
    private final String query;
    private final Map<String, ParameterBindingDTO> bindValues;

    /**
     * A split source with pre-computed splits from the Snowflake response.
     *
     * <p>Null until the Snowflake query is run and completes, then non-null and effectively final.
     */
    @Nullable
    private FixedSplitSource delegateSplitSource;

    SnowflakeParallelSplitSource(
            ConnectorSession session,
            Connection connection,
            SFSession sfSession,
            String query,
            Map<String, ParameterBindingDTO> bindValues)
    {
        this.session = session;
        this.connection = connection;
        this.sfSession = sfSession;
        this.sfStatement = new SFStatement(sfSession);
        this.query = query;
        this.bindValues = bindValues;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return getSplitSource().getNextBatch(maxSize);
    }

    private FixedSplitSource getSplitSource()
    {
        if (delegateSplitSource == null) {
            JsonNode jsonResult;
            try {
                jsonResult = (JsonNode) sfStatement.executeHelper(
                        query,
                        "application/snowflake",
                        bindValues,
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
