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
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.UuidType;

import java.util.Base64;
import java.util.UUID;

import static com.fasterxml.jackson.databind.node.JsonNodeType.STRING;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public final class StringColumnWriters
{
    public static final ColumnWriter BYTE_COLUMN_WRITER = new AbstractColumnWriter(VARBINARY, STRING)
    {
        @Override
        public String toString()
        {
            return "BYTE_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            final byte[] bytes;
            try {
                bytes = Base64.getDecoder().decode(node.textValue());
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "JSON STRING did not contain valid base64 encoded data",
                        e);
            }
            VARBINARY.writeSlice(
                    blockBuilder,
                    Slices.wrappedBuffer(bytes));
        }
    };

    public static final ColumnWriter UUID_COLUMN_WRITER = new AbstractColumnWriter(UuidType.UUID, STRING)
    {
        @Override
        public String toString()
        {
            return "UUID_COLUMN_WRITER";
        }

        @Override
        protected void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node)
        {
            final UUID uuid;
            try {
                uuid = UUID.fromString(node.textValue());
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(
                        OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                        "String was not a valid UUID",
                        e);
            }
            UuidType.UUID.writeSlice(blockBuilder, javaUuidToTrinoUuid(uuid));
        }
    };

    private StringColumnWriters()
    {
    }
}
