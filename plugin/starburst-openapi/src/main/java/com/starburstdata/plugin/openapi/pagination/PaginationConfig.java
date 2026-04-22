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

public class PaginationConfig
{
    public enum PaginationType
    {
        NONE,
        OFFSET,
        PAGE_NUMBER,
        LINK_HEADER,
        NEXT_CURSOR_FIELD,
        LAST_ELEMENT_CURSOR_FIELD,
        NEXT_URL_FIELD,
    }

    private PaginationType type = PaginationType.NONE;

    @NotNull
    public PaginationType getPaginationType()
    {
        return type;
    }

    @Config("openapi.pagination")
    @ConfigDescription("Pagination strategy used for API requests")
    public PaginationConfig setPaginationType(PaginationType type)
    {
        this.type = type;
        return this;
    }
}
