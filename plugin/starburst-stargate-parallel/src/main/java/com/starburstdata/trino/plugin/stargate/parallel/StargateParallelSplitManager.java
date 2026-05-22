/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StargateParallelSplitManager
        implements ConnectorSplitManager
{
    private final StargateClientFactory clientFactory;
    private final JdbcClient stargateClient;
    private final RemoteQueryModifier queryModifier;
    private final JdbcSplitManager jdbcSplitManager;
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(StargateParallelSplitManager.class.getName() + "-%d"));

    @Inject
    public StargateParallelSplitManager(
            StargateClientFactory clientFactory,
            JdbcClient stargateClient,
            RemoteQueryModifier queryModifier,
            JdbcSplitManager jdbcSplitManager)
    {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.stargateClient = requireNonNull(stargateClient, "stargateClient is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns, Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilterColumns, constraint);
        }

        // Synthetic handles represent operations that haven't been pushed down (sort, aggregations etc.)
        // In Stargate Parallel the only thing "parallel" is the data transfer - each split doesn't result in its own table scan
        // which makes it safe to generate multiple splits even for synthetic handles because exactly the same data is returned in the parallel and no-parallel paths.
        return new StargateParallelSplitSource(executor, clientFactory, stargateClient, queryModifier, session, (JdbcTableHandle) table);
    }
}
