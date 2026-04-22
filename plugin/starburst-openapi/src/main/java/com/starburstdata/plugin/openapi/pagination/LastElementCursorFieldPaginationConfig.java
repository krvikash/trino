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

import java.util.Optional;

public class LastElementCursorFieldPaginationConfig
{
    private String cursorParameterName;
    private String dataFieldJsonPointer;
    private String cursorFieldJsonPointer;

    @NotNull
    public String getCursorParameterName()
    {
        return cursorParameterName;
    }

    @Config("openapi.pagination.last-element-cursor-field.cursor-parameter-name")
    @ConfigDescription("Query parameter name used to pass the cursor to the next page request")
    public LastElementCursorFieldPaginationConfig setCursorParameterName(String cursorParameterName)
    {
        this.cursorParameterName = cursorParameterName;
        return this;
    }

    public Optional<String> getDataFieldJsonPointer()
    {
        return Optional.ofNullable(dataFieldJsonPointer);
    }

    @Config("openapi.pagination.last-element-cursor-field.data-field-json-pointer")
    @ConfigDescription("JSON Pointer (RFC 6901) to the response array whose last element provides the next cursor (e.g. /data); when omitted the response body itself is treated as the array")
    public LastElementCursorFieldPaginationConfig setDataFieldJsonPointer(String dataFieldJsonPointer)
    {
        this.dataFieldJsonPointer = dataFieldJsonPointer;
        return this;
    }

    @NotNull
    public String getCursorFieldJsonPointer()
    {
        return cursorFieldJsonPointer;
    }

    @Config("openapi.pagination.last-element-cursor-field.cursor-field-json-pointer")
    @ConfigDescription("JSON Pointer (RFC 6901) to the field within the last array element that contains the cursor value (e.g. /id)")
    public LastElementCursorFieldPaginationConfig setCursorFieldJsonPointer(String cursorFieldJsonPointer)
    {
        this.cursorFieldJsonPointer = cursorFieldJsonPointer;
        return this;
    }
}
