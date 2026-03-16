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
package io.trino.testing.mock;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.mock.CompiledWithOssCustomType.CUSTOM_TYPE;

public class CompiledWithOssMetadata
        implements ConnectorMetadata
{
    private static final String SCHEMA_NAME = "default";
    private static final String TABLE_NAME = "test_table";
    private static final String COLUMN1_NAME = "col1";
    private static final String COLUMN2_NAME = "col2";
    private static final String COLUMN3_NAME = "col3";

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (SCHEMA_NAME.equals(tableName.getSchemaName()) && TABLE_NAME.equals(tableName.getTableName())) {
            return new CompiledWithOssTableHandle(SCHEMA_NAME, TABLE_NAME);
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        CompiledWithOssTableHandle tableHandle = (CompiledWithOssTableHandle) table;
        return new ConnectorTableMetadata(
                new SchemaTableName(tableHandle.schemaName(), tableHandle.tableName()),
                List.of(new ColumnMetadata(COLUMN1_NAME, VARCHAR), new ColumnMetadata(COLUMN2_NAME, VARCHAR), new ColumnMetadata(COLUMN3_NAME, CUSTOM_TYPE)));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Map.of(
                COLUMN1_NAME, new CompiledWithOssColumnHandle(COLUMN1_NAME),
                COLUMN2_NAME, new CompiledWithOssColumnHandle(COLUMN2_NAME),
                COLUMN3_NAME, new CompiledWithOssColumnHandle(COLUMN3_NAME));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        CompiledWithOssColumnHandle column = (CompiledWithOssColumnHandle) columnHandle;
        if (COLUMN3_NAME.equals(column.columnName())) {
            return new ColumnMetadata(column.columnName(), CUSTOM_TYPE);
        }
        return new ColumnMetadata(column.columnName(), VARCHAR);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isEmpty() || SCHEMA_NAME.equals(schemaName.get())) {
            return List.of(new SchemaTableName(SCHEMA_NAME, TABLE_NAME));
        }
        return List.of();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if ((prefix.getSchema().isEmpty() || SCHEMA_NAME.equals(prefix.getSchema().get())) &&
                (prefix.getTable().isEmpty() || TABLE_NAME.equals(prefix.getTable().get()))) {
            return Map.of(
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                    List.of(new ColumnMetadata(COLUMN1_NAME, VARCHAR), new ColumnMetadata(COLUMN2_NAME, VARCHAR), new ColumnMetadata(COLUMN3_NAME, CUSTOM_TYPE)));
        }
        return Map.of();
    }
}
