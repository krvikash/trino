/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import io.trino.spi.connector.ConnectorSplit;

public enum OpenApiRequestSplit implements ConnectorSplit
{
    INSTANCE;
    @Override
    public long getRetainedSizeInBytes()
    {
        return 0;
    }
}
