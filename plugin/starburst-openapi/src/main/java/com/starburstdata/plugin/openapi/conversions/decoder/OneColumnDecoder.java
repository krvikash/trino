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
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SourcePage;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.singletonIterator;
import static java.util.Objects.requireNonNull;

/**
 * An OpenApiDecoder that decodes values into one column.
 */
public class OneColumnDecoder
        implements OpenApiDecoder
{
    private final ColumnWriter columnWriter;
    private final OpenApiColumnHandle columnHandle;
    private final List<OpenApiColumnHandle> columnHandles;
    private final RowSplitter rowSplitter;

    public OneColumnDecoder(ColumnWriter columnWriter, RowSplitter rowSplitter)
    {
        this.columnWriter = requireNonNull(columnWriter, "columnWriter is null");
        this.columnHandle = new OpenApiColumnHandle("value", columnWriter.getType());
        this.columnHandles = ImmutableList.of(columnHandle);
        this.rowSplitter = requireNonNull(rowSplitter, "rowSplitter is null");
    }

    @Override
    public List<OpenApiColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public Iterator<SourcePage> decodeFromRoot(JsonNode root, List<ColumnHandle> columnHandles)
    {
        checkArgument(
                columnHandles.stream().allMatch(columnHandle::equals),
                "Expected only single value column handle");
        int expectedRowCount = rowSplitter.expectedRowCount(root);
        if (columnHandles.isEmpty()) {
            return singletonIterator(SourcePage.create(expectedRowCount));
        }
        PageBuilder pageBuilder = new PageBuilder(Collections.nCopies(columnHandles.size(), columnWriter.getType()));
        return new PageByRowIterator(rowSplitter.rowIterator(root), pageBuilder) {
            @Override
            void writeToPageBuilder(JsonNode row, PageBuilder pageBuilder)
            {
                for (int channel = 0; channel < columnHandles.size(); channel++) {
                    columnWriter.writeToBuilder(pageBuilder.getBlockBuilder(channel), row);
                }
            }
        };
    }
}
