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

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class CompiledWithOssPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Map<String, List<String>> COLUMN_DATA = Map.of(
            "col1", List.of("hello", "world"),
            "col2", List.of("foo", "bar"));

    private static final Map<String, Type> COLUMN_TYPES = Map.of(
            "col1", VARCHAR,
            "col2", VARCHAR);

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        int rowCount = 2;
        BlockBuilder[] blockBuilders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String columnName = ((CompiledWithOssColumnHandle) columns.get(i)).columnName();
            Type type = COLUMN_TYPES.get(columnName);
            blockBuilders[i] = type.createBlockBuilder(null, rowCount);
        }

        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < columns.size(); col++) {
                String columnName = ((CompiledWithOssColumnHandle) columns.get(col)).columnName();
                Type type = COLUMN_TYPES.get(columnName);
                type.writeSlice(blockBuilders[col], io.airlift.slice.Slices.utf8Slice(COLUMN_DATA.get(columnName).get(row)));
            }
        }

        Page page = new Page(rowCount, java.util.Arrays.stream(blockBuilders)
                .map(BlockBuilder::build)
                .toArray(io.trino.spi.block.Block[]::new));
        return new FixedPageSource(List.of(page));
    }
}
