/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import io.starburst.schema.discovery.generation.Mapping;
import io.starburst.schema.discovery.models.DiscoveredTable;

import static io.starburst.schema.discovery.generation.Mapping.columnType;

public final class SchemaDiscoveryMappings
{
    private SchemaDiscoveryMappings() {}

    public static String sqlType(Column column)
    {
        return columnType(column);
    }

    public static String tableFormat(DiscoveredTable table)
    {
        return Mapping.tableFormat(table);
    }
}
