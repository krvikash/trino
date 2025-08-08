/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import io.trino.spi.connector.ConnectorSplit;

import static java.util.Objects.requireNonNull;

public record StorageSplit(String location)
        implements ConnectorSplit
{
    public StorageSplit
    {
        requireNonNull(location, "location is null");
    }
}
