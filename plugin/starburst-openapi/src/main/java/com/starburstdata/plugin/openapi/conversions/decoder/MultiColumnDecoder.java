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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SourcePage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_AMBIGUOUS_REFERENCE;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MultiColumnDecoder
        implements OpenApiDecoder
{
    private final Map<String, ColumnWriter> keyToColumnWriter;
    private final Map<String, String> identifierToKey;
    private final List<OpenApiColumnHandle> columnHandles;
    private final RowSplitter rowSplitter;

    /**
     * @param keyToColumnWriter Keys expected in the JSON objects from {@code rowSplitter}
     *         mapped to how their values should be converted to columns.
     * @param rowSplitter How to split a root JSON value into zero or more JSON values to be converted to rows.
     */
    public MultiColumnDecoder(
            Map<String, ColumnWriter> keyToColumnWriter,
            RowSplitter rowSplitter)
    {
        this.keyToColumnWriter = requireNonNull(keyToColumnWriter, "keyToColumnWriter is null");
        Map<String, String> mutableIdentifierToKey = new HashMap<>();
        ImmutableList.Builder<OpenApiColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        keyToColumnWriter.keySet().stream().sorted().forEach(key -> {
            String identifier = key.toLowerCase(ENGLISH);
            String oldKey = mutableIdentifierToKey.put(identifier, key);
            if (oldKey != null) {
                throw new TrinoException(
                        OPENAPI_AMBIGUOUS_REFERENCE,
                        "Column %s for property %s cannot be unambiguously referenced because of existing property %s".formatted(
                                identifier,
                                key,
                                oldKey));
            }
            columnHandlesBuilder.add(new OpenApiColumnHandle(identifier, keyToColumnWriter.get(key).getType()));
        });
        this.identifierToKey = ImmutableMap.copyOf(mutableIdentifierToKey);
        this.columnHandles = columnHandlesBuilder.build();
        this.rowSplitter = requireNonNull(rowSplitter, "rowSplitter is null");
    }

    @Override
    public List<OpenApiColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    /**
     * Converts one or more JSON objects into rows with multiple columns.
     * <p>
     * If a key/value is expected but missing, the column value is written as a null.
     */
    @Override
    public Iterator<SourcePage> decodeFromRoot(
            JsonNode root,
            List<ColumnHandle> columnHandles)
    {
        int expectedRowCount = rowSplitter.expectedRowCount(root);
        if (columnHandles.isEmpty()) {
            return singletonIterator(SourcePage.create(expectedRowCount));
        }

        ImmutableList.Builder<ColumnWriter> columnWritersBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> keysBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            if (columnHandle instanceof OpenApiColumnHandle castedHandle) {
                String identifier = castedHandle.name();
                String key = identifierToKey.get(identifier);
                keysBuilder.add(key);
                columnWritersBuilder.add(keyToColumnWriter.get(key));
            }
            else {
                throw new IllegalStateException("Selected unexpected column handle type %s".formatted(
                        columnHandle.getClass().getCanonicalName()));
            }
        }

        List<ColumnWriter> columnWriters = columnWritersBuilder.build();
        List<String> keys = keysBuilder.build();
        PageBuilder pageBuilder = new PageBuilder(
                columnWriters.stream()
                        .map(ColumnWriter::getType)
                        .collect(toImmutableList()));
        return new PageByRowIterator(rowSplitter.rowIterator(root), pageBuilder)
        {
            @Override
            void writeToPageBuilder(JsonNode row, PageBuilder pageBuilder)
            {
                if (!row.isObject()) {
                    throw new TrinoException(
                            OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                            "Expected JSON OBJECT but was %s".formatted(
                                    row.getNodeType().name()));
                }
                for (int channel = 0; channel < columnHandles.size(); channel++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                    String key = keys.get(channel);
                    JsonNode value = row.get(key);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        ColumnWriter columnWriter = columnWriters.get(channel);
                        columnWriter.writeToBuilder(blockBuilder, value);
                    }
                }
            }
        };
    }
}
