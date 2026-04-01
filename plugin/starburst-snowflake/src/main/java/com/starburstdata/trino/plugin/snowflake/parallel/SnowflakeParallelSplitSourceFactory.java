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
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.StarburstSnowflakeStatementV1;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient.throwIfInvalidWarehouse;
import static com.starburstdata.trino.plugin.snowflake.parallel.ChunkParser.parseChunks;
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeColumns.getPrimaryKeys;
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeColumns.getScanColumns;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SnowflakeParallelSplitSourceFactory
{
    private static final Logger LOG = Logger.get(SnowflakeParallelSplitSource.class);
    private final ConnectionFactory connectionFactory;
    private final SnowflakeClient snowflakeClient;
    private final RemoteQueryModifier queryModifier;

    @Inject
    public SnowflakeParallelSplitSourceFactory(ConnectionFactory connectionFactory, SnowflakeClient snowflakeClient, RemoteQueryModifier queryModifier)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.snowflakeClient = requireNonNull(snowflakeClient, "snowflakeClient is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
    }

    public SnowflakeParallelSplitSource create(
            ConnectorSession session,
            JdbcTableHandle table,
            DynamicFilter dynamicFilter)
    {
        // Synthetic handles represent operations that haven't been pushed down (sort, aggregations etc.)
        // In Snowflake Parallel the only thing "parallel" is the data transfer - each split doesn't result in its own table scan
        // which makes it safe to generate multiple splits even for synthetic handles because exactly the same data is returned in the parallel and no-parallel paths.
        try (Connection connection = connectionFactory.openConnection(session)) {
            List<JdbcColumnHandle> columns =
                    getScanColumns(
                            table.getColumns().map(List::copyOf).orElseGet(() -> snowflakeClient.getColumns(session, table)),
                            () -> getPrimaryKeys(session, snowflakeClient, table));

            PreparedQuery preparedQuery = snowflakeClient.prepareQuery(
                    session,
                    connection,
                    dynamicFilteringEnabled(session) ? table.intersectedWithConstraint(dynamicFilter.getCurrentPredicate()) : table,
                    columns,
                    Optional.empty());

            Map<String, ParameterBindingDTO> bindValues = convertToSnowflakeFormatWithStatement(preparedQuery, session, connection);
            SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
            SFStatement sFStatement = new SFStatement(sfSession);

            JsonNode jsonResult = (JsonNode) sFStatement.executeHelper(
                    preparedQuery.query(),
                    "application/snowflake",
                    bindValues,
                    false,
                    false,
                    false,
                    new ExecTimeTelemetryData());

            logFiltered(jsonResult);

            return new SnowflakeParallelSplitSource(parseChunks(session, jsonResult, sfSession));
        }
        catch (SFException | SQLException e) {
            // TODO: https://starburstdata.atlassian.net/browse/SEP-6500
            throwIfInvalidWarehouse(e);
            throw new TrinoException(JDBC_ERROR, "Couldn't get Snowflake splits, %s".formatted(e.getMessage()), e);
        }
    }

    /**
     * This is a duplication of {@link io.trino.plugin.jdbc.DefaultQueryBuilder}
     * Potential Trino change: QueryBuilder.prepareStatement(client, ...) -> QueryBuilder.prepareStatement(statement, ...)
     * would fix the duplication, but it won't be consistent with remaining class methods
     */
    private Map<String, ParameterBindingDTO> convertToSnowflakeFormatWithStatement(PreparedQuery preparedQuery, ConnectorSession session, Connection connection)
            throws SQLException
    {
        String modifiedQuery = queryModifier.apply(session, preparedQuery.query());
        StarburstSnowflakeStatementV1 statement = new StarburstSnowflakeStatementV1(connection.unwrap(SnowflakeConnectionV1.class), modifiedQuery);

        List<QueryParameter> parameters = preparedQuery.parameters();
        for (int i = 0; i < parameters.size(); i++) {
            QueryParameter parameter = parameters.get(i);
            int parameterIndex = i + 1;
            WriteFunction writeFunction = parameter.getJdbcType()
                    .map(jdbcType -> getWriteFunction(session, connection, jdbcType, parameter.getType()))
                    .orElseGet(() -> getWriteFunction(session, parameter.getType()));
            Class<?> javaType = writeFunction.getJavaType();
            Object value = parameter.getValue()
                    // The value must be present, since DefaultQueryBuilder never creates null parameters. Values coming from Domain's ValueSet are non-null, and
                    // nullable domains are handled explicitly, with SQL syntax.
                    .orElseThrow(() -> new VerifyException("Value is missing"));
            if (javaType == boolean.class) {
                ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, (boolean) value);
            }
            else if (javaType == long.class) {
                ((LongWriteFunction) writeFunction).set(statement, parameterIndex, (long) value);
            }
            else if (javaType == double.class) {
                ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, (double) value);
            }
            else if (javaType == Slice.class) {
                ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, (Slice) value);
            }
            else {
                ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, value);
            }
        }

        return statement.getParameterBindings();
    }

    private WriteFunction getWriteFunction(ConnectorSession session, Connection connection, JdbcTypeHandle jdbcTypeHandle, Type type)
    {
        ColumnMapping columnMapping = snowflakeClient.toColumnMapping(session, connection, jdbcTypeHandle)
                .orElseThrow(() -> new VerifyException(format("Unsupported type %s with handle %s", type, jdbcTypeHandle)));
        WriteFunction writeFunction = columnMapping.getWriteFunction();
        verify(writeFunction.getJavaType() == type.getJavaType(), "Java type mismatch: %s, %s", writeFunction, type);
        return writeFunction;
    }

    private WriteFunction getWriteFunction(ConnectorSession session, Type type)
    {
        return snowflakeClient.toWriteMapping(session, type).getWriteFunction();
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
