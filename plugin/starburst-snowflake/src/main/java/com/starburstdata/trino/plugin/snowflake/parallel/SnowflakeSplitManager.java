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

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import static java.util.Objects.requireNonNull;

public class SnowflakeSplitManager
        implements ConnectorSplitManager
{
    private final JdbcSplitManager jdbcSplitManager;
    private final SnowflakeParallelSplitSourceFactory parallelSplitSourceFactory;

    @Inject
    public SnowflakeSplitManager(
            JdbcSplitManager jdbcSplitManager,
            SnowflakeParallelSplitSourceFactory parallelSplitSourceFactory)
    {
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
        this.parallelSplitSourceFactory = parallelSplitSourceFactory;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }
        return parallelSplitSourceFactory.create(session, (JdbcTableHandle) table, dynamicFilter);
    }
}
