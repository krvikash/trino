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
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;

import static com.fasterxml.jackson.databind.node.JsonNodeType.NUMBER;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;

public final class NumberColumnWriters
{
    public static final ColumnWriter NUMBER_COLUMN_WRITER = new AbstractColumnWriter(NumberType.NUMBER, NUMBER)
    {
        @Override
        public String toString()
        {
            return "NUMBER_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            NumberType.NUMBER.writeObject(blockBuilder, TrinoNumber.from(node.decimalValue()));
        }
    };

    public static final ColumnWriter INTEGER_NUMBER_COLUMN_WRITER = new AbstractColumnWriter(NumberType.NUMBER, NUMBER)
    {
        @Override
        public String toString()
        {
            return "INTEGER_NUMBER_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            BigDecimal decimal = node.decimalValue();
            if (decimal.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Expected JSON NUMBER to be integral number");
            }
            NumberType.NUMBER.writeObject(blockBuilder, TrinoNumber.from(decimal));
        }
    };

    private NumberColumnWriters() {}
}
