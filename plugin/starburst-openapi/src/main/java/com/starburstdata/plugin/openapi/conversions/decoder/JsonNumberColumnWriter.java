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
import io.trino.spi.type.Type;

import java.math.BigDecimal;

import static com.fasterxml.jackson.databind.node.JsonNodeType.NUMBER;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static io.airlift.slice.Slices.utf8Slice;

public class JsonNumberColumnWriter
        extends AbstractColumnWriter
{
    private final boolean integer;

    public JsonNumberColumnWriter(Type jsonType, boolean integer)
    {
        // Numbers in JSON are unbounded, so one option is to map to JSON to not truncate data.
        super(jsonType, NUMBER);
        this.integer = integer;
    }

    @Override
    public void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
    {
        if (integer && node.decimalValue().remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
            throw new TrinoException(
                    OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                    "Expected JSON NUMBER to be integral number");
        }
        getType().writeSlice(blockBuilder, utf8Slice(node.toString()));
    }
}
