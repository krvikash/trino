/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SalesforceCdpJdbcClient
        extends BaseJdbcClient
{
    private static final String SCHEMA_NAME = "queryservice";
    private static final Pattern OBJECT_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z_0-9]*$");

    @Inject
    public SalesforceCdpJdbcClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            @ForSalesforceCdpIdentifierMapping IdentifierMapping identifierMapping,
            QueryBuilder queryBuilder,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        verify(config.getJdbcTypesMappedToVarchar().isEmpty(), "Salesforce CDP does not support forced type mapping to VARCHAR");
    }

    @Override
    protected String getTableRemoteSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // Salesforce Customer Data Platform returns null for schema
        return SCHEMA_NAME;
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        // Salesforce Customer Data Platform returns empty list for schemas
        return ImmutableSet.of(SCHEMA_NAME);
    }

    @Override
    protected String escapeObjectNameForMetadataQuery(String name, String escape)
    {
        // noop. Salesforce Customer Data Platform JDBC Metadata returns null for the databaseMetadata.getSearchStringEscape()
        verify(OBJECT_NAME_PATTERN.matcher(name).matches(), "Invalid object name: %s", name);
        return name;
    }

    @Override
    protected PreparedQuery prepareQuery(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions,
            Optional<JdbcSplit> split)
    {
        RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
        List<JdbcColumnHandle> mappedColumns = columns.stream()
                .map(column -> toRemoteColumnHandle(column, remoteIdentifiers))
                .collect(toImmutableList());
        Optional<List<List<JdbcColumnHandle>>> mappedGroupingSets = groupingSets
                .map(groupingSet -> groupingSet.stream()
                        .map(group -> group.stream()
                                .map(column -> toRemoteColumnHandle(column, remoteIdentifiers))
                                .collect(toImmutableList()))
                        .collect(toImmutableList()));
        JdbcTableHandle preparedTable = table.isNamedRelation() ? prepareTableHandleForQuery(table, connection) : table;
        return super.prepareQuery(session, connection, preparedTable, mappedGroupingSets, mappedColumns, columnExpressions, split);
    }

    private JdbcColumnHandle toRemoteColumnHandle(JdbcColumnHandle column, RemoteIdentifiers remoteIdentifiers)
    {
        return JdbcColumnHandle.builderFrom(column)
                .setColumnName(getIdentifierMapping().toRemoteColumnName(remoteIdentifiers, column.getColumnName()))
                .build();
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        return new SalesforceCdpEmulatedPreparedStatement(connection, sql);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return super.getColumns(session, tableHandle).stream()
                .map(column -> JdbcColumnHandle.builderFrom(column)
                        .setColumnName(getIdentifierMapping().fromRemoteColumnName(column.getColumnName()))
                        .build())
                .collect(toImmutableList());
    }

    private JdbcTableHandle prepareTableHandleForQuery(JdbcTableHandle table, Connection connection)
    {
        JdbcNamedRelationHandle relation = table.getRequiredNamedRelation();
        RemoteTableName remoteTableName = relation.getRemoteTableName();
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        checkArgument(schemaName == null, "Salesforce CDP remote schema is expected to be null");

        RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
        Optional<List<JdbcColumnHandle>> mappedColumns = table.getColumns()
                .map(columns -> columns.stream()
                        .map(column -> toRemoteColumnHandle(column, remoteIdentifiers))
                        .collect(toImmutableList()));

        TupleDomain<ColumnHandle> mappedConstraint = table.getConstraint();
        if (table.getConstraint().getDomains().isPresent()) {
            Map<ColumnHandle, Domain> collect = table.getConstraint().getDomains().get().entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> {
                                JdbcColumnHandle column = (JdbcColumnHandle) entry.getKey();
                                return toRemoteColumnHandle(column, remoteIdentifiers);
                            },
                            Map.Entry::getValue));
            mappedConstraint = TupleDomain.withColumnDomains(collect);
        }

        return new JdbcTableHandle(
                new JdbcNamedRelationHandle(
                        relation.getSchemaTableName(),
                        // Salesforce Customer Data Platform doesn't support table names to be qualified with catalog and schema names in the SQL query, hence we null out both.
                        new RemoteTableName(
                                Optional.empty(),
                                Optional.empty(),
                                remoteTableName.getTableName()),
                        Optional.empty()),
                mappedConstraint,
                table.getConstraintExpressions(),
                table.getSortOrder(),
                table.getLimit(),
                mappedColumns,
                table.getOtherReferencedTables(),
                table.getNextSyntheticColumnId(),
                table.getAuthorization(),
                table.getUpdateAssignments());
    }

    // this is overridden because the resultset for metadata.getColumns() returned from the driver contains
    // a hardcoded catalog name called "catalog", whereas the RemoteTableName in tableHandle (aka from getTables()) has a hardcoded catalog name "QueryService".
    // Since the driver is not being consistent here, we cannot simply rely on what resultSet.getString("TABLE_CAT")
    @Override
    protected RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        return new RemoteTableName(
                Optional.of("QueryService"),
                Optional.ofNullable(resultSet.getString("TABLE_SCHEM")),
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Consumer<Runnable> rollbackActionConsumer)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void rollbackTemporaryTableCreation(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN -> Optional.of(booleanColumnMapping());
            case Types.NUMERIC -> Optional.of(doubleColumnMapping());
            // the column_size in the driver is hard-coded to 255 and the actual data can overflow
            // Set CASE_INSENSITIVE_CHARACTER_PUSHDOWN conservatively.
            case Types.VARCHAR -> Optional.of(ColumnMapping.sliceMapping(
                    createUnboundedVarcharType(),
                    unboundedVarcharReadFunction(),
                    varcharWriteFunction(),
                    CASE_INSENSITIVE_CHARACTER_PUSHDOWN));
            case Types.DATE -> Optional.of(ColumnMapping.longMapping(
                    DATE,
                    dateReadFunctionUsingSqlDate(),
                    dateWriteFunctionUsingLocalDate()));
            case Types.TIMESTAMP -> Optional.of(ColumnMapping.longMapping(
                    TIMESTAMP_TZ_MILLIS,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction()));
            default -> Optional.empty();
        };
        if (mapping.isPresent()) {
            return mapping;
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            // Salesforce CDP driver does not support resultSet.getObject(int columnIndex, Class<T> type) so we cannot use resultSet.getObject(columnIndex, LocalDateTime.class)
            Instant instant = resultSet.getTimestamp(columnIndex).toInstant();
            ZonedDateTime zonedDateTime = instant.atZone(ZoneOffset.UTC);
            if (instant.getNano() % 1_000_000 != 0) {
                throw new TrinoException(NOT_SUPPORTED, "Timestamp with higher precision than milliseconds is not supported timestamp: " + instant);
            }
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, Instant.ofEpochMilli(millisUtc).atZone(timeZoneKey.getZoneId()));
        };
    }

    private static LongReadFunction dateReadFunctionUsingSqlDate()
    {
        // Not using StandardColumnMappings#dateReadFunctionUsingSqlDate as it assumes getting timestamp at midnight in JVM timezone and converts it to UTC.
        // Salesforce returns hour according to timezone and above logic causes wrong results.
        return (resultSet, columnIndex) -> MILLISECONDS.toDays(resultSet.getDate(columnIndex).getTime());
    }

    private static SliceReadFunction unboundedVarcharReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(requireNonNull(resultSet.getObject(columnIndex), "value is null").toString());
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support writing");
    }
}
