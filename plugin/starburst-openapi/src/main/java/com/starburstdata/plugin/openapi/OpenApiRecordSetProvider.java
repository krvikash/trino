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
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OpenApiRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final OpenApiClient client;
    private final OpenApiSpec spec;

    @Inject
    public OpenApiRecordSetProvider(OpenApiSpec spec, OpenApiClient client)
    {
        this.spec = requireNonNull(spec, "spec is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columnHandles)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) table;
        ConnectorTableMetadata tableMetadata = spec.getTableMetadata(tableHandle.schemaTableName());
        Map<String, Integer> columnIndexByName = IntStream.range(0, tableMetadata.getColumns().size()).boxed()
                .collect(Collectors.toMap(i -> tableMetadata.getColumns().get(i).getName(), i -> i));

        List<Integer> columnIndexes = new ArrayList<>(columnHandles.size());
        ImmutableList.Builder<Type> mappedTypes = ImmutableList.builderWithExpectedSize(columnHandles.size());
        for (ColumnHandle columnHandle : columnHandles) {
            OpenApiColumnHandle column = (OpenApiColumnHandle) columnHandle;
            Integer index = columnIndexByName.get(column.name().toLowerCase(ENGLISH));
            verify(index != null, "Column %s not found in %s", column.name(), columnIndexByName.keySet());
            columnIndexes.add(index);
            mappedTypes.add(column.type());
        }

        OpenApiSplit split = (OpenApiSplit) connectorSplit;
        Iterable<List<?>> rows = client.getRows(tableHandle.schemaTableName(), tableHandle.selectPaths(), tableHandle.selectMethod(), split.getConstraint());
        Iterable<List<?>> mappedRows = Iterables.transform(rows, row -> columnIndexes
                .stream()
                .map(row::get)
                .collect(toList()));

        return new InMemoryRecordSet(mappedTypes.build(), mappedRows);
    }
}
