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

import java.math.BigDecimal;

import static com.fasterxml.jackson.databind.node.JsonNodeType.NUMBER;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;

public final class FloatingPointNumberColumnWriters
{
    public static final ColumnWriter FLOAT_COLUMN_WRITER = new AbstractColumnWriter(REAL, NUMBER)
    {
        @Override
        public String toString()
        {
            return "FLOAT_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            BigDecimal decimalValue = node.decimalValue();
            float floatValue = node.floatValue();
            if (Float.isInfinite(floatValue)) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Failed to fit decimal value within bounds of real type");
            }
            if (BigDecimal.valueOf(floatValue).compareTo(decimalValue) != 0) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Would lose floating point precision fitting decimal value in real type");
            }
            REAL.writeFloat(blockBuilder, floatValue);
        }
    };

    public static final ColumnWriter DOUBLE_COLUMN_WRITER = new AbstractColumnWriter(DOUBLE, NUMBER)
    {
        @Override
        public String toString()
        {
            return "DOUBLE_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            BigDecimal decimalValue = node.decimalValue();
            double doubleValue = decimalValue.doubleValue();
            if (Double.isInfinite(doubleValue)) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Failed to fit decimal value within bounds of double type");
            }
            if (BigDecimal.valueOf(doubleValue).compareTo(decimalValue) != 0) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Would lose floating point precision fitting decimal value in double type");
            }
            DOUBLE.writeDouble(blockBuilder, doubleValue);
        }
    };

    private FloatingPointNumberColumnWriters()
    {
    }
}
