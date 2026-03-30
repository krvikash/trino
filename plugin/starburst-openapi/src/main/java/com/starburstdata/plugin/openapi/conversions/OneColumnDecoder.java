/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SourcePage;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.singletonIterator;
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
    public Iterator<SourcePage> decodeFromRoot(JsonNode root, List<ColumnHandle> columnHandles)
    {
        if (columnHandles.isEmpty()) {
            return singletonIterator(SourcePage.create(1));
        }
        checkArgument(
                columnHandles.stream().allMatch(SINGLE_COLUMN_HANDLE::equals),
                "Expected only single value column handle");
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeString(blockBuilder, root.toString());
        Block block = blockBuilder.build();
        Block[] blocks = new Block[columnHandles.size()];
        Arrays.fill(blocks, block);
        return singletonIterator(SourcePage.create(new Page(blocks)));
    }
}
