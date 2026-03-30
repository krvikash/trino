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
import com.starburstdata.plugin.openapi.conversions.decoder.ColumnWriter;
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
import static java.util.Objects.requireNonNull;

/**
 * An OpenApiDecoder that decodes values into one column with one row.
 */
public class OneColumnDecoder
        implements OpenApiDecoder
{
    private final ColumnWriter columnWriter;
    private final OpenApiColumnHandle columnHandle;
    private final List<OpenApiColumnHandle> columnHandles;

    public OneColumnDecoder(ColumnWriter columnWriter)
    {
        this.columnWriter = requireNonNull(columnWriter, "columnWriter is null");
        this.columnHandle = new OpenApiColumnHandle("value", columnWriter.getType());
        this.columnHandles = ImmutableList.of(columnHandle);
    }

    @Override
    public List<OpenApiColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public Iterator<SourcePage> decodeFromRoot(JsonNode root, List<ColumnHandle> columnHandles)
    {
        checkArgument(
                columnHandles.stream().allMatch(columnHandle::equals),
                "Expected only single value column handle");
        if (columnHandles.isEmpty()) {
            return singletonIterator(SourcePage.create(1));
        }
        BlockBuilder blockBuilder = columnWriter.getType().createBlockBuilder(null, 1);
        columnWriter.writeToBuilder(blockBuilder, root);
        Block block = blockBuilder.build();
        Block[] blocks = new Block[columnHandles.size()];
        Arrays.fill(blocks, block);
        return singletonIterator(SourcePage.create(new Page(1, blocks)));
    }
}
