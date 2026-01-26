/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerMultiDatabaseClient
        extends StarburstSqlServerClient
{
    @VisibleForTesting
    static final String DATABASE_SEPARATOR = ".";

    private static final Splitter DATABASE_SPLITTER = Splitter.on(DATABASE_SEPARATOR);
    private final IdentifierMapping identifierMapping;

    @Inject
    public StarburstSqlServerMultiDatabaseClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, statisticsConfig, connectionFactory, queryBuilder, identifierMapping, queryModifier);
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        Collection<String> catalogNames = listCatalogs(connection);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (String catalogName : catalogNames) {
            String catalogId = escapeSQLId(catalogName);
            // Avoid using DatabaseMetaData.getSchemas method because
            // https://github.com/microsoft/mssql-jdbc/commit/351c2bec2ed88249cf9d68804224b717015d1625 introduced a filtering
            // of internal (predefined) schemas.
            try (PreparedStatement statement = connection.prepareStatement("SELECT name FROM " + catalogId + ".sys.schemas");
                    ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String schemaName = resultSet.getString("name");
                    // skip internal schemas
                    if (filterRemoteSchema(schemaName)) {
                        schemaNames.add(format("%s%s%s", catalogName, DATABASE_SEPARATOR, schemaName));
                    }
                }
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
        return schemaNames.build();
    }

    /**
     * Copy of com.microsoft.sqlserver.jdbc.Util.escapeSQLId
     */
    private static String escapeSQLId(String inID)
    {
        // SQL bracket escaping rules.
        // Given <identifier> yields -> [<identifier>]
        // Where <identifier> is first escaped to replace all
        // instances of "]" with "]]".
        // For example, column name "abc" -> "[abc]"
        // For example, column name "]" -> "[]]]"
        // For example, column name "]ab]cd" -> "[]]ab]]cd]"
        char ch;

        // Add 2 extra chars for open and closing brackets.
        StringBuilder outID = new StringBuilder(inID.length() + 2);

        outID.append('[');
        for (int i = 0; i < inID.length(); i++) {
            ch = inID.charAt(i);
            if (']' == ch) {
                outID.append("]]");
            }
            else {
                outID.append(ch);
            }
        }
        outID.append(']');
        return outID.toString();
    }

    private Collection<String> listCatalogs(Connection connection)
    {
        try (Statement statement = connection.createStatement();
                // We are performing ORDER BY as DatabaseMetaData#getCatalogs returns catalog name in a ordered fashion
                ResultSet resultSet = statement.executeQuery("SELECT name AS TABLE_CAT FROM sys.databases WHERE HAS_DBACCESS(name) = 1 ORDER BY name")) {
            ImmutableSet.Builder<String> catalogNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String catalogName = resultSet.getString("TABLE_CAT");
                catalogNames.add(catalogName);
            }
            return catalogNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteSchemaName);
        connection.setCatalog(databaseSchema.databaseName);
        super.createSchema(session, connection, databaseSchema.schemaName);
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteSchemaName);
        connection.setCatalog(databaseSchema.databaseName);
        super.dropSchema(session, connection, databaseSchema.schemaName, cascade);
    }

    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName, Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        ConnectorIdentity identity = session.getIdentity();
        SchemaTableName schemaTableName = tableMetadata.getTable();
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(schemaTableName.getSchemaName());

        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            connection.setCatalog(databaseSchema.databaseName);
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, databaseSchema.schemaName);
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String remoteTargetTableName = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, targetTableName);
            String catalog = connection.getCatalog();

            verifyTableName(connection.getMetaData(), remoteTargetTableName);

            return createTable(
                    session,
                    connection,
                    tableMetadata,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    remoteTargetTableName,
                    pageSinkIdColumn);
        }
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(schemaTableName.getSchemaName());

        ConnectorIdentity identity = session.getIdentity();
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            connection.setCatalog(databaseSchema.databaseName);
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, databaseSchema.schemaName);
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String catalog = connection.getCatalog();

            JdbcOutputTableHandle table = beginInsertTable(
                    session,
                    connection,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columns);
            this.enableTableLockOnBulkLoadTableOption(session, table);
            return table;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(
            ConnectorSession session,
            Connection connection,
            String catalogName,
            String remoteSchemaName,
            String remoteTableName,
            String newRemoteSchemaName,
            String newRemoteTableName)
            throws SQLException
    {
        DatabaseSchemaName newDatabaseSchema = parseDatabaseSchemaName(newRemoteSchemaName);
        if (!catalogName.equals(newDatabaseSchema.databaseName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across catalogs");
        }
        super.renameTable(
                session,
                connection,
                catalogName,
                remoteSchemaName,
                remoteTableName,
                newDatabaseSchema.schemaName,
                newRemoteTableName);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        Optional<DatabaseSchemaName> databaseSchema = remoteSchemaName.map(StarburstSqlServerMultiDatabaseClient::parseDatabaseSchemaName);
        return getTablesInternal(
                connection,
                databaseSchema.map(DatabaseSchemaName::databaseName).orElse(null),
                databaseSchema.map(DatabaseSchemaName::schemaName), remoteTableName);
    }

    @Override
    public String getTableRemoteSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT") + DATABASE_SEPARATOR + resultSet.getString("TABLE_SCHEM");
    }

    private static DatabaseSchemaName parseDatabaseSchemaName(String schemaName)
    {
        List<String> databaseSchemaName = DATABASE_SPLITTER.splitToList(schemaName);
        if (databaseSchemaName.size() < 2) {
            throw new TrinoException(INVALID_ARGUMENTS, "The expected format is '<database name>.<schema name>': " + schemaName);
        }
        if (databaseSchemaName.size() > 2) {
            throw new TrinoException(INVALID_ARGUMENTS, "Too many identifier parts found");
        }
        return new DatabaseSchemaName(databaseSchemaName.get(0), databaseSchemaName.get(1));
    }

    record DatabaseSchemaName(String databaseName, String schemaName) {}

    @Override
    public RemoteIdentifiers getRemoteIdentifiers(Connection connection)
    {
        return new RemoteIdentifiers()
        {
            @Override
            public Set<String> getRemoteSchemas()
            {
                return ImmutableSet.copyOf(listSchemas(connection));
            }

            @Override
            public Set<String> getRemoteTables(String remoteSchema)
            {
                try (ResultSet resultSet = getTablesInternal(connection, connection.getCatalog(), Optional.of(remoteSchema), Optional.empty())) {
                    ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
                    while (resultSet.next()) {
                        tableNames.add(resultSet.getString("TABLE_NAME"));
                    }
                    return tableNames.build();
                }
                catch (SQLException e) {
                    throw new TrinoException(JDBC_ERROR, e);
                }
            }

            @Override
            public boolean storesUpperCaseIdentifiers()
            {
                return false;
            }
        };
    }

    private ResultSet getTablesInternal(Connection connection, String remoteDatabase, Optional<String> remoteSchema, Optional<String> remoteTableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                remoteDatabase,
                escapeObjectNameForMetadataQuery(remoteSchema, metadata.getSearchStringEscape()).orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }
}
