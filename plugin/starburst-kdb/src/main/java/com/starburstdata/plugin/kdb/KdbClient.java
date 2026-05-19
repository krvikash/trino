/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.plugin.kdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.kx.c;
import com.kx.c.Flip;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.starburstdata.plugin.kdb.KdbErrorCode.KDB_CONNECTION_ERROR;
import static com.starburstdata.plugin.kdb.KdbErrorCode.KDB_QUERY_ERROR;
import static com.starburstdata.plugin.kdb.KdbTypeMapping.toColumnMapping;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class KdbClient
{
    private static final Logger log = Logger.get(KdbClient.class);

    // KDB+ built-in system namespaces that contain functions/internals, not user tables.
    // See: https://code.kx.com/q/basics/namespaces/#system-namespaces
    // j (JSON) and s (streaming) were added in later KDB+ versions.
    private static final Set<String> SYSTEM_NAMESPACES = ImmutableSet.of("q", "Q", "h", "j", "o", "s", "z");

    private final KdbConnectionFactory connectionFactory;

    @Inject
    public KdbClient(KdbConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    public Object execute(String query)
    {
        log.debug("Executing KDB query: %s", query);
        c connection = connectionFactory.openConnection();
        try {
            return connection.k(query);
        }
        catch (c.KException e) {
            throw new TrinoException(KDB_QUERY_ERROR, "KDB+ error executing query: %s".formatted(query), e);
        }
        catch (IOException e) {
            throw new TrinoException(KDB_CONNECTION_ERROR, "Network error executing KDB+ query: %s".formatted(query), e);
        }
        finally {
            try {
                connection.close();
            }
            catch (IOException e) {
                log.debug(e, "Failed to close KDB+ connection after query");
            }
        }
    }

    /**
     * key` returns ALL names in the root namespace: tables, variables, functions,
     * and namespace dictionaries. Filter to only names that KDB+ recognises as
     * namespaces by calling tables on the corresponding namespace symbol
     * (sv join name to get .name). tables return a symbol vector (type 11h) for
     * valid namespaces and throw for non-namespace names; protected eval
     * turns errors into a short null (type -5h != 11h).
     */
    public List<String> listSchemas()
    {
        Object result = execute("{x where{11h=type @[tables;` sv `,' x;{0Nh}]}each x} key `");
        if (!(result instanceof String[] namespaces)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Unexpected result type listing KDB+ namespaces: " + result.getClass().getName());
        }

        ImmutableList.Builder<String> schemas = ImmutableList.builder();
        schemas.add("default");

        for (String namespace : namespaces) {
            if (!namespace.isEmpty() && !SYSTEM_NAMESPACES.contains(namespace)) {
                schemas.add(namespace);
            }
        }

        return schemas.build();
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        List<String> schemas = schemaName
                .<List<String>>map(ImmutableList::of)
                .orElseGet(this::listSchemas);

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();

        for (String schema : schemas) {
            try {
                validateIdentifier(schema, "schema");
                String query;
                if (schema.equals("default")) {
                    query = "tables[]";
                }
                else {
                    // Namespace symbols require the leading dot: `.ns not `ns
                    query = "tables[`.%s]".formatted(schema);
                }

                Object result = execute(query);
                String[] tableNames;
                if (result instanceof String[] array) {
                    tableNames = array;
                }
                else if (result instanceof Flip) {
                    // the namespace itself is a table, not a container
                    continue;
                }
                else {
                    tableNames = new String[0];
                }

                for (String tableName : tableNames) {
                    tables.add(new SchemaTableName(schema, tableName));
                }
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_USER_ERROR, "Failed to list tables for schema: %s".formatted(schema), e);
            }
        }

        return tables.build();
    }

    public List<KdbColumnHandle> loadColumns(SchemaTableName tableName)
    {
        validateIdentifier(tableName.getSchemaName(), "schema");
        validateIdentifier(tableName.getTableName(), "table");
        String fullTableName = getFullTableName(tableName);

        try {
            String query = "0!meta %s".formatted(fullTableName);
            Object result = execute(query);

            if (!(result instanceof Flip flip)) {
                throw new TrinoException(NOT_FOUND, "Table not found or invalid: %s".formatted(tableName));
            }

            String[] colNames = flip.x;
            Object[] colData = flip.y;

            int cIndex = -1;
            int tIndex = -1;

            for (int i = 0; i < colNames.length; i++) {
                if ("c".equals(colNames[i])) {
                    cIndex = i;
                }
                else if ("t".equals(colNames[i])) {
                    tIndex = i;
                }
            }

            if (cIndex == -1 || tIndex == -1) {
                throw new TrinoException(GENERIC_USER_ERROR, "Invalid meta result for table: %s".formatted(tableName));
            }

            String[] columnNames = (String[]) colData[cIndex];
            char[] columnTypes = (char[]) colData[tIndex];

            ImmutableList.Builder<KdbColumnHandle> columns = ImmutableList.builder();
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                char kdbType = columnTypes[i];
                Optional<ColumnMapping> mapping = toColumnMapping(kdbType);
                if (mapping.isPresent()) {
                    columns.add(new KdbColumnHandle(columnName, mapping.get().type(), kdbType, i));
                }
            }

            return columns.build();
        }
        catch (TrinoException e) {
            // kdb+ query error (e.g. table doesn't exist) - treat as NOT_FOUND
            if (e.getErrorCode().equals(KDB_QUERY_ERROR.toErrorCode())) {
                throw new TrinoException(NOT_FOUND, "Table not found or invalid: %s".formatted(tableName), e.getCause());
            }
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_USER_ERROR,
                    "Failed to get metadata for table: %s".formatted(tableName), e);
        }
    }

    private static String getFullTableName(SchemaTableName tableName)
    {
        if (tableName.getSchemaName().equals("default")) {
            // Root-level tables are referenced by bare name. Using the symbol form
            // (`tablename) breaks the KDB+ drop operator
            return tableName.getTableName();
        }
        // Namespace tables are referenced using the fully-qualified dotted path
        // (.ns.table). The symbol form (`ns.table / `.ns.table) does not work
        // in this KDB+ version; value on namespace symbols also fails.
        return ".%s.%s".formatted(tableName.getSchemaName(), tableName.getTableName());
    }

    public KdbQueryResult fetchData(SchemaTableName tableName, List<KdbColumnHandle> columns)
    {
        String query = buildQuery(tableName, columns);

        Object result = execute(query);

        if (!(result instanceof Flip flip)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Expected table result from KDB query");
        }

        return new KdbQueryResult(flip);
    }

    private static String buildQuery(SchemaTableName tableName, List<KdbColumnHandle> columns)
    {
        validateIdentifier(tableName.getSchemaName(), "schema");
        validateIdentifier(tableName.getTableName(), "table");
        String fullTableName = getFullTableName(tableName);

        if (columns.isEmpty()) {
            return "select from %s".formatted(fullTableName);
        }

        // Use functional select form to safely handle reserved column names
        // ?[table; (); 0b; cols!cols] projects named columns
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                cols.append("`");
            }
            String columnName = columns.get(i).columnName();
            validateIdentifier(columnName, "column");
            cols.append(columnName);
        }
        String colList = columns.size() == 1 ? "enlist[`" + cols + "]" : "`" + cols;
        return "?[%s; (); 0b; %s!%s]".formatted(fullTableName, colList, colList);
    }

    public static boolean isValidIdentifier(String name)
    {
        return name.matches("[a-zA-Z0-9_]+");
    }

    private static void validateIdentifier(String name, String type)
    {
        if (!isValidIdentifier(name)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Invalid %s name: %s".formatted(type, name));
        }
    }

    public boolean tableExists(SchemaTableName tableName)
    {
        validateIdentifier(tableName.getSchemaName(), "schema");
        validateIdentifier(tableName.getTableName(), "table");
        String schema = tableName.getSchemaName();
        String table = tableName.getTableName();
        // tables[] returns a symbol list; `name in tables[...] is a boolean membership test.
        // This is cheaper than 0!meta since it doesn't fetch column types.
        String query = schema.equals("default")
                ? "`%s in tables[]".formatted(table)
                : "`%s in tables[`.%s]".formatted(table, schema);
        Object result = execute(query);
        return result instanceof Boolean b && b;
    }

    public static class KdbQueryResult
    {
        private final String[] columnNames;
        private final Object[] columnData;
        private final int rowCount;

        public KdbQueryResult(Flip flip)
        {
            columnNames = flip.x;
            columnData = flip.y;

            if (columnData.length > 0 && columnData[0] != null) {
                rowCount = Array.getLength(columnData[0]);
            }
            else {
                rowCount = 0;
            }
        }

        public String[] columnNames()
        {
            return columnNames.clone();
        }

        public int rowCount()
        {
            return rowCount;
        }

        public Object value(int columnIndex, int rowIndex)
        {
            Object column = columnData[columnIndex];
            return Array.get(column, rowIndex);
        }
    }
}
