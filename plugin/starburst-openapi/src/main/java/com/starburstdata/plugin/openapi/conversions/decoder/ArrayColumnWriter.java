/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static java.util.Objects.requireNonNull;

public class ArrayColumnWriter
        extends AbstractColumnWriter
{
    private final ColumnWriter itemsColumnWriter;

    public ArrayColumnWriter(ColumnWriter itemsColumnWriter)
    {
        super(new ArrayType(itemsColumnWriter.getType()), ARRAY);
        this.itemsColumnWriter = requireNonNull(itemsColumnWriter, "itemsColumnWriter is null");
    }

    @Override
    public void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
    {
        ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) blockBuilder;
        arrayBlockBuilder.buildEntry(arrayItemBuilder -> {
            for (int element = 0; element < node.size(); element++) {
                itemsColumnWriter.writeToBuilder(arrayItemBuilder, node.get(element));
            }
        });
    }
}
