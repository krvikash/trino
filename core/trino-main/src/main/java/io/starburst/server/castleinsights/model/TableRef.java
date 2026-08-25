/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.model;

import static java.util.Objects.requireNonNull;

public record TableRef(String catalog, String schema, String table)
{
    public TableRef
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
    }

    @Override
    public String toString()
    {
        return catalog + "." + schema + "." + table;
    }
}
