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
import com.starburstdata.plugin.openapi.OpenApiErrorCode;
import io.trino.spi.TrinoException;

import java.util.Iterator;

import static com.google.common.collect.Iterators.singletonIterator;

/**
 * @param isRootArray Whether the root JSON value is expected to be an array whose values should convert to rows.
 */
public record RowSplitter(boolean isRootArray)
{
    public static final RowSplitter ARRAY_SPLITTER = new RowSplitter(true);
    public static final RowSplitter LINE_SPLITTER = new RowSplitter(false);

    public int expectedRowCount(JsonNode root)
    {
        if (isRootArray && !root.isArray()) {
            throw new TrinoException(
                    OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                    "Expected JSON ARRAY but was %s".formatted(root.getNodeType().name()));
        }
        if (isRootArray) {
            return root.size();
        }
        return 1;
    }

    public Iterator<JsonNode> rowIterator(JsonNode root)
    {
        if (isRootArray) {
            return root.elements();
        }
        return singletonIterator(root);
    }
}
