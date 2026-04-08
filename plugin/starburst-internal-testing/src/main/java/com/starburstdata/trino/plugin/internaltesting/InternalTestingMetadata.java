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
package com.starburstdata.trino.plugin.internaltesting;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class InternalTestingMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(InternalTestingMetadata.class);
    private static final String OOM_SCHEMA_NAME = "oom";
    private static final List<String> OOM_PLACES = ImmutableList.of("coordinator", "worker");

    private final String oomTestString = "HEAP_DUMP_COORDINATOR_TEST_SENSITIVE_STRING_DATA";
    private final String[] oomTestStringArray = {"HEAP_DUMP_COORDINATOR_TEST_ARRAY_ELEMENT_1", "HEAP_DUMP_COORDINATOR_TEST_ARRAY_ELEMENT_2"};
    private final InternalTestingMemoryAllocator memoryAllocator;

    @Inject
    public InternalTestingMetadata(InternalTestingMemoryAllocator memoryAllocator)
    {
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocation is null");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && OOM_SCHEMA_NAME.equals(schemaName.get())) {
            return OOM_PLACES.stream().map(oomPlace -> new SchemaTableName(OOM_SCHEMA_NAME, oomPlace)).collect(toImmutableList());
        }
        return ImmutableList.of();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ImmutableMap.of("value", new InternalTestingColumnHandle("value", BIGINT));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        InternalTestingColumnHandle internalTestingColumnHandle = (InternalTestingColumnHandle) columnHandle;
        return new ColumnMetadata(internalTestingColumnHandle.name(), internalTestingColumnHandle.columnType());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(OOM_SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (OOM_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            if ("coordinator".equals(tableName.getTableName())) {
                log.info("getTableHandle called for coordinator table. Triggering OOM");
                memoryAllocator.allocate();
            }
            if ("worker".equals(tableName.getTableName())) {
                log.info("getTableHandle called for worker table. Triggering OOM");
                return new InternalTestingTableHandle(OOM_SCHEMA_NAME, "worker");
            }
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof InternalTestingTableHandle) {
            return new ConnectorTableMetadata(
                    new SchemaTableName(OOM_SCHEMA_NAME, "worker"),
                    ImmutableList.of(new ColumnMetadata("value", BIGINT)));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unknown table handle: " + table);
    }

    public String getOOMTestString()
    {
        return oomTestString;
    }

    public String[] getOOMTestStringArray()
    {
        return oomTestStringArray;
    }
}
