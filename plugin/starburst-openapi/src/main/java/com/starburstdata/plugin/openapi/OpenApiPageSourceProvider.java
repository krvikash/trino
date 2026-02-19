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
import com.google.inject.Inject;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
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
import io.trino.spi.connector.RecordPageSource;

import java.util.Arrays;
import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OpenApiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final OpenApiRecordSetProvider recordSetProvider;

    @Inject
    public OpenApiPageSourceProvider(OpenApiRecordSetProvider recordSetProvider)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        return switch (table) {
            case OpenApiTableHandle _ -> new RecordPageSource(recordSetProvider.getRecordSet(
                    transaction,
                    session,
                    split,
                    table,
                    columns));
            case OpenApiRequestTableHandle _ -> {
                BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 0);
                VARCHAR.writeString(blockBuilder, "TODO");
                Block block = blockBuilder.build();
                Block[] blocks = new Block[columns.size()];
                Arrays.fill(blocks, block);
                yield new FixedPageSource(ImmutableList.of(new Page(blocks)));
            }
            default -> throw new IllegalArgumentException(format(
                    "Unknown table class: %s",
                    table.getClass().getCanonicalName()));
        };
    }
}
