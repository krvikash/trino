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

public class NextUrlFieldPaginationConfig
{
    private String nextUrlFieldJsonPointer;

    @NotNull
    public String getNextUrlFieldJsonPointer()
    {
        return nextUrlFieldJsonPointer;
    }

    @Config("openapi.pagination.next-url-cursor.next-url-field-json-pointer")
    @ConfigDescription("JSON Pointer (RFC 6901) to the field in the response body containing the absolute URL for the next page (e.g. /paging/next)")
    public NextUrlFieldPaginationConfig setNextUrlFieldJsonPointer(String nextUrlFieldJsonPointer)
    {
        this.nextUrlFieldJsonPointer = nextUrlFieldJsonPointer;
        return this;
    }
}
