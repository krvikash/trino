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
import com.google.common.collect.AbstractIterator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SourcePage;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public abstract class PageByRowIterator
        extends AbstractIterator<SourcePage>
{
    private final Iterator<JsonNode> rowIterator;
    private final PageBuilder pageBuilder;

    public PageByRowIterator(
            Iterator<JsonNode> rowIterator,
            PageBuilder pageBuilder)
    {
        this.rowIterator = requireNonNull(rowIterator, "rowIterator is null");
        this.pageBuilder = requireNonNull(pageBuilder, "pageBuilder is null");
    }

    @Override
    protected SourcePage computeNext()
    {
        while (rowIterator.hasNext() && !pageBuilder.isFull()) {
            JsonNode row = rowIterator.next();
            pageBuilder.declarePosition();
            writeToPageBuilder(row, pageBuilder);
        }
        if (pageBuilder.isEmpty()) {
            return endOfData();
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    abstract void writeToPageBuilder(JsonNode row, PageBuilder pageBuilder);
}
