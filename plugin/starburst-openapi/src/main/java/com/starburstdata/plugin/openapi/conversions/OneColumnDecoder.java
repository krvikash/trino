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
package com.starburstdata.plugin.openapi.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class OneColumnDecoder
        implements OpenApiDecoder
{
    private static final OpenApiColumnHandle SINGLE_COLUMN_HANDLE = new OpenApiColumnHandle("value", VARCHAR);
    private static final List<OpenApiColumnHandle> COLUMNS = ImmutableList.of(SINGLE_COLUMN_HANDLE);

    @Override
    public List<OpenApiColumnHandle> getColumnHandles()
    {
        return COLUMNS;
    }

    @Override
    public Page decodeToPage(JsonNode root, List<ColumnHandle> columnHandles)
    {
        if (columnHandles.isEmpty()) {
            return new Page(1);
        }
        checkArgument(
                columnHandles.stream().allMatch(SINGLE_COLUMN_HANDLE::equals),
                "Expected only single value column handle");
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeString(blockBuilder, root.toString());
        Block block = blockBuilder.build();
        Block[] blocks = new Block[columnHandles.size()];
        Arrays.fill(blocks, block);
        return new Page(blocks);
    }
}
