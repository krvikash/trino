/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class NextCursorFieldPaginationConfig
{
    private String cursorParameterName;
    private String cursorFieldJsonPointer;

    @NotNull
    public String getCursorParameterName()
    {
        return cursorParameterName;
    }

    @Config("openapi.pagination.next-field-cursor.cursor-parameter-name")
    @ConfigDescription("Query parameter name for the pagination cursor")
    public NextCursorFieldPaginationConfig setCursorParameterName(String cursorParameterName)
    {
        this.cursorParameterName = cursorParameterName;
        return this;
    }

    @NotNull
    public String getCursorFieldJsonPointer()
    {
        return cursorFieldJsonPointer;
    }

    @Config("openapi.pagination.next-field-cursor.cursor-field-json-pointer")
    @ConfigDescription("JSON Pointer (RFC 6901) to the field in the response body containing the next cursor (e.g. /response_metadata/next_cursor)")
    public NextCursorFieldPaginationConfig setCursorFieldJsonPointer(String cursorFieldJsonPointer)
    {
        this.cursorFieldJsonPointer = cursorFieldJsonPointer;
        return this;
    }
}
