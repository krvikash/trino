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
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SourcePage;

import java.util.Iterator;
import java.util.List;

public interface OpenApiDecoder
{
    List<OpenApiColumnHandle> getColumnHandles();

    /**
     * @param root The root {@link JsonNode} of the API response.
     * @param columnHandles A selection of column handles from {@link #getColumnHandles()}
     *         that represents the order of columns in the SourcePages.
     */
    Iterator<SourcePage> decodeFromRoot(JsonNode root, List<ColumnHandle> columnHandles);
}
