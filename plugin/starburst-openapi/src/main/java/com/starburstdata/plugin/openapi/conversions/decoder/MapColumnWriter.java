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
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;

import java.util.Iterator;

import static com.fasterxml.jackson.databind.node.JsonNodeType.OBJECT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class MapColumnWriter
        extends AbstractColumnWriter
{
    private final ColumnWriter valueColumnWriter;

    public MapColumnWriter(ColumnWriter valueColumnWriter, TypeOperators typeOperators)
    {
        super(
                new MapType(
                        VARCHAR,
                        requireNonNull(valueColumnWriter, "valueColumnWriter is null").getType(),
                        typeOperators),
                OBJECT);
        this.valueColumnWriter = requireNonNull(valueColumnWriter, "valueColumnWriter is null");
    }

    @Override
    protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
    {
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
        mapBlockBuilder.buildEntry((keyBlockBuilder, valueBlockBuilder) -> {
            Iterator<String> fieldNameIterator = node.fieldNames();
            while (fieldNameIterator.hasNext()) {
                String fieldName = fieldNameIterator.next();
                valueColumnWriter.writeToBuilder(valueBlockBuilder, node.get(fieldName));
                VARCHAR.writeString(keyBlockBuilder, fieldName);
            }
        });
    }
}
