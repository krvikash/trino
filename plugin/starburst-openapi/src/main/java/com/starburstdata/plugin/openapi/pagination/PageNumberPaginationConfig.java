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

public class PageNumberPaginationConfig
{
    private String pageParameterName;
    private String isLastPageFieldJsonPointer;

    @NotNull
    public String getPageParameterName()
    {
        return pageParameterName;
    }

    @Config("openapi.pagination.page-number.page-parameter-name")
    @ConfigDescription("Query parameter name for the page number")
    public PageNumberPaginationConfig setPageParameterName(String pageParameterName)
    {
        this.pageParameterName = pageParameterName;
        return this;
    }

    @NotNull
    public String getIsLastPageFieldJsonPointer()
    {
        return isLastPageFieldJsonPointer;
    }

    @Config("openapi.pagination.page-number.is-last-page-field-json-pointer")
    @ConfigDescription("JSON Pointer (RFC 6901) to the boolean field in the response body indicating whether the last page has been reached (e.g. /last)")
    public PageNumberPaginationConfig setIsLastPageFieldJsonPointer(String isLastPageFieldJsonPointer)
    {
        this.isLastPageFieldJsonPointer = isLastPageFieldJsonPointer;
        return this;
    }
}
