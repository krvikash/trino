/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.function.Supplier;

import static io.trino.plugin.jdbc.DefaultJdbcMetadata.MERGE_ROW_ID;
import static java.util.function.Predicate.not;

class SnowflakeColumns
{
    private SnowflakeColumns() {}

    static List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, JdbcClient client, JdbcTableHandle table)
    {
        return client.getPrimaryKeys(session, table.getRequiredNamedRelation().getRemoteTableName());
    }

    /**
     * Replaces the merge-specific column id with the actual primary key columns,  ensuring the correct columns are used during the scan.
     */
    static List<JdbcColumnHandle> getScanColumns(
            List<JdbcColumnHandle> columns,
            Supplier<List<JdbcColumnHandle>> primaryKeysSupplier)
    {
        if (columns.stream().noneMatch(column -> column.getColumnName().equalsIgnoreCase(MERGE_ROW_ID))) {
            return columns;
        }
        ImmutableList.Builder<JdbcColumnHandle> columnsBuilder = ImmutableList.builder();
        columns.stream()
                .filter(not(column -> column.getColumnName().equalsIgnoreCase(MERGE_ROW_ID)))
                .forEach(columnsBuilder::add);
        primaryKeysSupplier.get().stream().filter(not(columns::contains)).forEach(columnsBuilder::add);
        return columnsBuilder.build();
    }
}
