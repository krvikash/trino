/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions.encoder;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

public class ListTypeEncoder
        implements TypeEncoder
{
    private final LeafTypeEncoder itemEncoder;
    private final Type arrayType;
    private final Type itemType;

    public ListTypeEncoder(LeafTypeEncoder itemEncoder)
    {
        this.itemEncoder = requireNonNull(itemEncoder, "itemEncoder is null");
        this.arrayType = new ArrayType(itemEncoder.getType());
        this.itemType = itemEncoder.getType();
    }

    @Override
    public Type getType()
    {
        return arrayType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SerializedValue serialize(Object value)
    {
        Block block = (Block) value;
        ImmutableList.Builder<String> valuesBuilder = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            Object itemValue = readNativeValue(itemType, block, position);
            valuesBuilder.add(itemEncoder.serializeToString(itemValue));
        }
        return new SerializedList(valuesBuilder.build());
    }
}
