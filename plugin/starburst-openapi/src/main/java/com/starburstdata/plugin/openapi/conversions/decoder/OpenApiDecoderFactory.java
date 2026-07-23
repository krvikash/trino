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
import com.starburstdata.plugin.openapi.conversions.ir.ObjectIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;

public class OpenApiDecoderFactory
{
    private final ColumnWriterFactory columnWriterFactory;

    @Inject
    public OpenApiDecoderFactory(ColumnWriterFactory columnWriterFactory)
    {
        this.columnWriterFactory = columnWriterFactory;
    }

    /**
     * Stably converts a {@link SchemaIr} into an {@link OpenApiDecoder}.
     * <p>
     * If the response is expected to be a JSON array,
     * this factory returns a decoder to turn each JSON value into a new row.
     * If the JSON values converted to rows are expected to be JSON objects with known keys,
     * then this factory returns a decoder to turn each key/value pair into a column.
     */
    public OpenApiDecoder createFrom(SchemaIr schemaIr)
    {
        final SchemaIr rowIr;
        final RowSplitter rowSplitter;

        if (schemaIr instanceof ArrayIr(SchemaIr setAsRowIr)) {
            rowIr = setAsRowIr;
            rowSplitter = RowSplitter.ARRAY_SPLITTER;
        }
        else {
            rowIr = schemaIr;
            rowSplitter = RowSplitter.LINE_SPLITTER;
        }

        if (rowIr instanceof ObjectIr objectIr && objectIr.strictlyNamedProperties()) {
            return new MultiColumnDecoder(
                    Maps.transformValues(objectIr.properties(), columnWriterFactory::createFrom),
                    rowSplitter);
        }
        return new OneColumnDecoder(columnWriterFactory.createFrom(rowIr), rowSplitter);
    }
}
