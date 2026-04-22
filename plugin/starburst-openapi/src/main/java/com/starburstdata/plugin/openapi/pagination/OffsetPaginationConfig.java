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

public class OffsetPaginationConfig
{
    private String offsetParameterName;
    private String dataFieldJsonPointer;

    @NotNull
    public String getOffsetParameterName()
    {
        return offsetParameterName;
    }

    @Config("openapi.pagination.offset.offset-parameter-name")
    @ConfigDescription("Query parameter name for the row offset")
    public OffsetPaginationConfig setOffsetParameterName(String offsetParameterName)
    {
        this.offsetParameterName = offsetParameterName;
        return this;
    }

    public Optional<String> getDataFieldJsonPointer()
    {
        return Optional.ofNullable(dataFieldJsonPointer);
    }

    @Config("openapi.pagination.offset.data-field-json-pointer")
    @ConfigDescription("JSON Pointer (RFC 6901) to the response array whose last element provides the next cursor (e.g. /data); when omitted the response body itself is treated as the array")
    public OffsetPaginationConfig setDataFieldJsonPointer(String dataFieldJsonPointer)
    {
        this.dataFieldJsonPointer = dataFieldJsonPointer;
        return this;
    }
}
