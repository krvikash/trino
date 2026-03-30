/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SourcePage;

import java.util.Iterator;
import java.util.List;

public interface OpenApiDecoder
{
    OpenApiDecoder ONE_COLUMN_DECODER = new OneColumnDecoder();

    List<OpenApiColumnHandle> getColumnHandles();

    Iterator<SourcePage> decodeFromRoot(JsonNode root, List<ColumnHandle> columnHandles);
}
