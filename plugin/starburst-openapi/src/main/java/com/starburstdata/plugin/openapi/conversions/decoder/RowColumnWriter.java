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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_AMBIGUOUS_REFERENCE;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNEXPECTED_RESPONSE_SCHEMA;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RowColumnWriter
        implements ColumnWriter
{
    private final Type type;
    private final List<String> keys;
    private final List<ColumnWriter> columnWriters;

    /**
     * @param keyToColumnWriter JSON object keys mapped to how the value should be converted as a nested column.
     */
    public RowColumnWriter(Map<String, ColumnWriter> keyToColumnWriter)
    {
        requireNonNull(keyToColumnWriter, "keyToColumnWriter is null");
        ImmutableList.Builder<String> keysBuilder = ImmutableList.builder();
        ImmutableMultimap.Builder<String, String> identifierKeyMultimap = ImmutableMultimap.builder();
        ImmutableList.Builder<ColumnWriter> columnWritersBuilder = ImmutableList.builder();
        ImmutableList.Builder<RowType.Field> fieldsBuilder = ImmutableList.builder();
        List<String> sortedKeys = keyToColumnWriter.keySet()
                .stream()
                .sorted(Comparator.comparing(key -> key.toLowerCase(ENGLISH)))
                .toList();
        for (String key : sortedKeys) {
            keysBuilder.add(key);
            ColumnWriter columnWriter = keyToColumnWriter.get(key);
            columnWritersBuilder.add(columnWriter);
            String identifier = key.toLowerCase(ENGLISH);
            identifierKeyMultimap.put(identifier, key);
            fieldsBuilder.add(RowType.field(identifier, columnWriter.getType()));
        }
        Map<String, Collection<String>> ambiguousFields = Maps.filterValues(
                identifierKeyMultimap.build().asMap(),
                list -> list.size() > 1);
        if (!ambiguousFields.isEmpty()) {
            throw new TrinoException(
                    OPENAPI_AMBIGUOUS_REFERENCE,
                    "Fields [%s] cannot be unambiguously referenced".formatted(
                            ambiguousFields.values()
                                    .stream()
                                    .flatMap(Collection::stream)
                                    .collect(Collectors.joining(", "))));
        }
        this.type = RowType.from(fieldsBuilder.build());
        this.columnWriters = columnWritersBuilder.build();
        this.keys = keysBuilder.build();
    }

    @Override
    public Type getType()
    {
        return type;
    }

    /**
     * Converts JSON objects to {@link RowType} values.
     * <p>If an expected key/value pair isn't present a null value is appended.
     */
    @Override
    public void writeToBuilder(
            BlockBuilder blockBuilder,
            JsonNode node)
    {
        if (node.isNull()) {
            blockBuilder.appendNull();
            return;
        }
        if (!node.isObject()) {
            throw new TrinoException(
                    OPENAPI_UNEXPECTED_RESPONSE_SCHEMA,
                    "Expected JSON OBJECT but was %s".formatted(node.getNodeType().name()));
        }
        RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) blockBuilder;
        rowBlockBuilder.buildEntry(fieldsBuilders -> {
            checkState(keys.size() == fieldsBuilders.size());
            for (int field = 0; field < fieldsBuilders.size(); field++) {
                BlockBuilder fieldBuilder = fieldsBuilders.get(field);
                String key = keys.get(field);
                JsonNode valueNode = node.get(key);
                if (valueNode == null) {
                    fieldBuilder.appendNull();
                    continue;
                }
                ColumnWriter columnWriter = columnWriters.get(field);
                columnWriter.writeToBuilder(fieldBuilder, valueNode);
            }
        });
    }
}
