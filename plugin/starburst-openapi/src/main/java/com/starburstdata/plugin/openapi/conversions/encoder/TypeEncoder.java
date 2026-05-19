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

import com.starburstdata.plugin.openapi.conversions.ir.ArrayIr;
import com.starburstdata.plugin.openapi.conversions.ir.LeafIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.List;

import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSUPPORTED_PARAMETER;

public interface TypeEncoder
{
    Type getType();

    SerializedValue serialize(Object value);

    sealed interface SerializedValue
            permits SerializedList, SerializedString {}

    record SerializedList(List<String> values)
            implements SerializedValue {}

    record SerializedString(String value)
            implements SerializedValue {}

    static TypeEncoder from(SchemaIr schemaIr)
    {
        return switch (schemaIr) {
            case ArrayIr(LeafIr leafIr) -> new ListTypeEncoder(LeafTypeEncoder.from(leafIr));
            case ArrayIr _ -> throw new TrinoException(
                    OPENAPI_UNSUPPORTED_PARAMETER,
                    "Cannot create a parameter from an array of a non-primitive type (supported string/number format or boolean)");
            case LeafIr leafIr -> LeafTypeEncoder.from(leafIr);
            default -> throw new TrinoException(
                    OPENAPI_UNSUPPORTED_PARAMETER,
                    "Must create a parameter from a primitive type (supported string/number format or boolean) or array of primitive type");
        };
    }
}
