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

import com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy;
import com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle.ParameterStyle;
import com.starburstdata.plugin.openapi.conversions.ir.ArrayIr;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import com.starburstdata.plugin.openapi.conversions.ir.LeafIr;
import com.starburstdata.plugin.openapi.conversions.ir.ObjectIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.Type;

import java.util.List;

import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.FALLBACK;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSUPPORTED_PARAMETER;
import static com.starburstdata.plugin.openapi.conversions.encoder.LeafTypeEncoder.STRING_ENCODER;

public interface TypeEncoder
{
    Type getType();

    /**
     * @param value The "native representation" (see {@link ScalarArgument#getValue()}) of a value of type {@link #getType()}.
     */
    SerializedValue serialize(Object value);

    /**
     * An intermediate representation of a value to be serialized and added to a header, cookie, etc.
     * <p>See {@link ParameterStyle} for ways a serialized value can be transformed.
     * These values should not be escaped.
     */
    sealed interface SerializedValue
            permits SerializedList, SerializedString {}

    record SerializedList(List<String> values)
            implements SerializedValue {}

    record SerializedString(String value)
            implements SerializedValue {}

    static TypeEncoder from(SchemaIr schemaIr, CastPolicy castPolicy)
    {
        return switch (schemaIr) {
            case ArrayIr(LeafIr leafIr) -> new ListTypeEncoder(LeafTypeEncoder.from(leafIr));
            case ArrayIr _, JsonIr _, ObjectIr _ when castPolicy == FALLBACK -> STRING_ENCODER;
            case ArrayIr _ -> throw new TrinoException(
                    OPENAPI_UNSUPPORTED_PARAMETER,
                    "Cannot create a parameter from an array of a non-primitive type (supported string/number format or boolean)");
            case LeafIr leafIr -> LeafTypeEncoder.from(leafIr);
            case JsonIr _, ObjectIr _ -> throw new TrinoException(
                    OPENAPI_UNSUPPORTED_PARAMETER,
                    "Must create a parameter from a primitive type (supported string/number format or boolean) or array of primitive type");
        };
    }
}
