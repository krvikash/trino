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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;

public record JsonColumnWriter(Type jsonType)
        implements ColumnWriter
{
    @Override
    public Type getType()
    {
        return jsonType;
    }

    @Override
    public void writeToBuilder(BlockBuilder blockBuilder, JsonNode node)
    {
        // NOTE: no null checking, we'll write the null literally.
        jsonType.writeSlice(blockBuilder, utf8Slice(node.toString()));
    }
}
