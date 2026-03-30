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

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.starburstdata.plugin.openapi.conversions.ir.ArrayIr;
import com.starburstdata.plugin.openapi.conversions.ir.BooleanIr;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import com.starburstdata.plugin.openapi.conversions.ir.NumberIr;
import com.starburstdata.plugin.openapi.conversions.ir.ObjectIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import static com.starburstdata.plugin.openapi.conversions.decoder.FloatingPointNumberColumnWriters.DOUBLE_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.FloatingPointNumberColumnWriters.FLOAT_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.IntegerColumnWriters.INT32_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.IntegerColumnWriters.INT64_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.SimpleColumnWriters.BOOLEAN_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.SimpleColumnWriters.STRING_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.StringColumnWriters.BYTE_COLUMN_WRITER;
import static com.starburstdata.plugin.openapi.conversions.decoder.StringColumnWriters.UUID_COLUMN_WRITER;
import static io.trino.spi.type.StandardTypes.JSON;
import static java.util.Objects.requireNonNull;

public class ColumnWriterFactory
{
    private final JsonColumnWriter jsonColumnWriter;
    private final JsonNumberColumnWriter jsonIntegerColumnWriter;
    private final JsonNumberColumnWriter jsonNumberColumnWriter;
    private final TypeOperators typeOperators;

    @Inject
    public ColumnWriterFactory(
            TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        Type jsonType = typeManager.getType(new TypeSignature(JSON));
        jsonColumnWriter = new JsonColumnWriter(jsonType);
        jsonIntegerColumnWriter = new JsonNumberColumnWriter(jsonType, true);
        jsonNumberColumnWriter = new JsonNumberColumnWriter(jsonType, false);
        typeOperators = typeManager.getTypeOperators();
    }

    public ColumnWriter createFrom(SchemaIr schemaIr)
    {
        return switch (schemaIr) {
            case ArrayIr arrayIr -> new ArrayColumnWriter(createFrom(arrayIr.items()));
            case ObjectIr objectIr -> createFromObject(objectIr);
            case BooleanIr _ -> BOOLEAN_COLUMN_WRITER;
            case JsonIr _ -> jsonColumnWriter;
            case StringIr stringIr -> createStringColumnWriter(stringIr.format());
            case NumberIr numberIr -> createNumberColumnWriter(numberIr.format());
        };
    }

    private ColumnWriter createFromObject(ObjectIr objectIr)
    {
        // TODO filter out writeOnly properties.
        if (objectIr.properties().isEmpty() && objectIr.additionalProperties().isEmpty()) {
            return jsonColumnWriter;
        }
        else if (objectIr.additionalProperties().isEmpty()) {
            return new RowColumnWriter(Maps.transformValues(objectIr.properties(), this::createFrom));
        }
        else if (objectIr.properties().isEmpty()) {
            return new MapColumnWriter(createFrom(objectIr.additionalProperties().get()), typeOperators);
        }
        // Value is union type of properties and additionalProperties, JSON is safest.
        return new MapColumnWriter(jsonColumnWriter, typeOperators);
    }

    private ColumnWriter createStringColumnWriter(StringIr.Format format)
    {
        return switch (format) {
            case NONE -> STRING_COLUMN_WRITER;
            case BYTE -> BYTE_COLUMN_WRITER;
            case UUID -> UUID_COLUMN_WRITER;
        };
    }

    private ColumnWriter createNumberColumnWriter(NumberIr.Format format)
    {
        return switch (format) {
            // JSON grammar allows unlimited scale and precision ...
            // So for safety, we first write to a JSON string.
            // TODO use the new NUMBER type once in cork.
            case NONE_INTEGER -> jsonIntegerColumnWriter;
            case NONE_NUMBER -> jsonNumberColumnWriter;
            case INT32 -> INT32_COLUMN_WRITER;
            case INT64 -> INT64_COLUMN_WRITER;
            case DOUBLE -> DOUBLE_COLUMN_WRITER;
            case FLOAT -> FLOAT_COLUMN_WRITER;
        };
    }
}
