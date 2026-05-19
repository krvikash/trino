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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class SimpleColumnWriters
{
    public static final ColumnWriter BOOLEAN_COLUMN_WRITER = new AbstractColumnWriter(BOOLEAN, JsonNodeType.BOOLEAN)
    {
        @Override
        public String toString()
        {
            return "BOOLEAN_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            BOOLEAN.writeBoolean(blockBuilder, node.booleanValue());
        }
    };

    public static final ColumnWriter STRING_COLUMN_WRITER = new AbstractColumnWriter(VARCHAR, JsonNodeType.STRING)
    {
        @Override
        public String toString()
        {
            return "STRING_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            VARCHAR.writeString(blockBuilder, node.textValue());
        }
    };

    private SimpleColumnWriters() {}
}
