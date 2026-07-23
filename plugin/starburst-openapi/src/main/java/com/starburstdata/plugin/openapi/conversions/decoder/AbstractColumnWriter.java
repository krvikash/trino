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
import com.fasterxml.jackson.databind.node.NullNode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static java.util.Objects.requireNonNull;

public abstract class AbstractColumnWriter
        implements ColumnWriter
{
    private final Type type;
    private final JsonNodeType expectedNodeType;

    protected AbstractColumnWriter(Type type, JsonNodeType expectedNodeType)
    {
        this.type = requireNonNull(type, "type is null");
        this.expectedNodeType = requireNonNull(expectedNodeType, "expectedNodeType is null");
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void writeToBuilder(BlockBuilder blockBuilder, JsonNode node)
    {
        if (node.isNull()) {
            blockBuilder.appendNull();
            return;
        }
        JsonNodeType actualNodeType = node.getNodeType();
        if (!expectedNodeType.equals(actualNodeType)) {
            throw new TrinoException(
                    OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                    "Expected JSON %s but was %s".formatted(expectedNodeType, actualNodeType));
        }
        writeToBuilderUnchecked(blockBuilder, node);
    }

    /**
     * Write the given {@link JsonNode} which is guaranteed to not be a {@link NullNode} and of the expected node type.
     */
    protected abstract void writeToBuilderUnchecked(BlockBuilder blockBuilder, JsonNode node);
}
