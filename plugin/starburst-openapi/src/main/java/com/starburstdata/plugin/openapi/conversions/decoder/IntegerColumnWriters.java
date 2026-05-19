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
import java.math.BigInteger;

import static com.fasterxml.jackson.databind.node.JsonNodeType.NUMBER;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;

public final class IntegerColumnWriters
{
    public static final ColumnWriter INT32_COLUMN_WRITER = new AbstractColumnWriter(INTEGER, NUMBER)
    {
        @Override
        public String toString()
        {
            return "INT32_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            BigInteger bigInteger = checkedBigIntegerValue(node);
            try {
                INTEGER.writeInt(blockBuilder, bigInteger.intValueExact());
            }
            catch (ArithmeticException e) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Failed to fit JSON NUMBER in integer type",
                        e);
            }
        }
    };

    public static final ColumnWriter INT64_COLUMN_WRITER = new AbstractColumnWriter(BIGINT, NUMBER)
    {
        @Override
        public String toString()
        {
            return "INT64_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            BigInteger bigInteger = checkedBigIntegerValue(node);
            try {
                BIGINT.writeLong(blockBuilder, bigInteger.longValueExact());
            }
            catch (ArithmeticException e) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "Failed to fit JSON NUMBER in bigint type",
                        e);
            }
        }
    };

    private static BigInteger checkedBigIntegerValue(JsonNode node)
    {
        if (node.decimalValue().remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
            throw new TrinoException(
                    OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                    "Expected JSON NUMBER as integral value");
        }
        return node.bigIntegerValue();
    }

    private IntegerColumnWriters() {}
}
